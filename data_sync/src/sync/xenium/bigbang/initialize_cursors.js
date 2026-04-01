/**
 * Xenium Bigbang — Initialize Poller Cursors
 *
 * Initializes `xenium_sync_cursor` rows for each xenium poller at the point
 * in time where the bigbang ended. This prevents pollers from re-processing
 * rows that were already migrated by bigbang.
 *
 * Uses the max `updated_at` + max `id` from each relevant xenium source table
 * as the initial cursor position (same bounded-window safety as CMG cursors).
 *
 * Implemented in Chat 2.
 */

const logger = require('../../../logger');
const { xeniumCursorManager } = require('../../shared/cursor_manager');
const { XENIUM_POLLER_NAMES } = require('../constants');
const { coerceIntegerId } = require('./helpers');

function coerceCursorId(value, idMode) {
  if (idMode === 'string') {
    // xenium_sync_cursor.last_xenium_id is currently INT, so string IDs cannot be stored here.
    // Keep last_updated_at but leave last_xenium_id null for string-backed source models.
    return null;
  }
  return coerceIntegerId(value);
}

async function getModelTailCursor(xeniumPrisma, modelName, idMode) {
  const row = await xeniumPrisma[modelName].findFirst({
    orderBy: [{ updated_at: 'desc' }, { id: 'desc' }],
    select: { id: true, updated_at: true },
  });

  if (!row || !row.updated_at) {
    return { lastUpdatedAt: null, lastXeniumId: null };
  }

  const lastXeniumId = coerceCursorId(row.id, idMode);
  if (idMode === 'int' && !lastXeniumId) {
    return { lastUpdatedAt: null, lastXeniumId: null };
  }

  return {
    lastUpdatedAt: row.updated_at,
    lastXeniumId,
  };
}

/**
 * @param {PrismaClient} prisma       Target Bioloop database
 * @param {PrismaClient} xeniumPrisma Source Xenium database
 * @returns {Promise<void>}
 */
async function initializeCursors(prisma, xeniumPrisma) {
  logger.info('[XENIUM][initialize_cursors] Initializing xenium poller cursors');

  const cursorSpecs = [
    { pollerName: XENIUM_POLLER_NAMES.USER_ROLES, modelName: 'user', idMode: 'int' },
    { pollerName: XENIUM_POLLER_NAMES.PROJECT_ACL, modelName: 'project', idMode: 'string' },
    { pollerName: XENIUM_POLLER_NAMES.DATASET_METADATA, modelName: 'dataset', idMode: 'int' },
    { pollerName: XENIUM_POLLER_NAMES.PROJECT_METADATA, modelName: 'project', idMode: 'string' },
  ];

  for (const spec of cursorSpecs) {
    // eslint-disable-next-line no-await-in-loop
    const cursor = await getModelTailCursor(xeniumPrisma, spec.modelName, spec.idMode);
    // eslint-disable-next-line no-await-in-loop
    await xeniumCursorManager.initializeCursor(
      prisma,
      spec.pollerName,
      cursor.lastUpdatedAt,
      cursor.lastXeniumId,
    );

    logger.info(
      `[XENIUM][initialize_cursors] ${spec.pollerName} => `
      + `updated_at=${cursor.lastUpdatedAt || 'null'}, `
      + `last_xenium_id=${cursor.lastXeniumId || 'null'}`,
    );
  }
}

module.exports = { initializeCursors };

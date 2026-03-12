/**
 * Xenium Bigbang — Seed Constants
 *
 * Creates roles, the xenium system user, analysis types, and import source records
 * in the target Bioloop database. These are prerequisites for all subsequent
 * xenium migration steps.
 *
 * Idempotent: checks for existing records before creating.
 *
 * Implemented in Chat 2 with idempotent create/update behavior.
 */

const logger = require('../../../logger');
const { XENIUM_USER, XENIUM_IMPORT_SOURCES } = require('../constants');
const { withDatasetOrigin } = require('./helpers');

/**
 * Ensure the three core roles (user, operator, admin) exist.
 * Reuses the same roles as the CMG bigbang — no new roles needed.
 *
 * @param {PrismaClient} prisma
 * @returns {Promise<void>}
 */
async function ensureRolesExist(prisma) {
  const roles = [
    { name: 'admin', description: 'Access to the Admin Panel' },
    { name: 'operator', description: 'Operator level access' },
    { name: 'user', description: 'User level access' },
  ];

  let created = 0;
  for (const role of roles) {
    // eslint-disable-next-line no-await-in-loop
    const existing = await prisma.role.findFirst({ where: { name: role.name } });
    if (!existing) {
      // eslint-disable-next-line no-await-in-loop
      await prisma.role.create({ data: role });
      created += 1;
    }
  }

  logger.info(`[XENIUM][seed_constants] ensureRolesExist complete (${created} created)`);
}

/**
 * Create the Xenium system user (username: 'xeniumuser') if it does not exist.
 *
 * @param {PrismaClient} prisma
 * @returns {Promise<number>} The Bioloop user ID of the xenium system user
 */
async function createXeniumUser(prisma) {
  let user = await prisma.user.findUnique({
    where: { username: XENIUM_USER.username },
  });

  if (!user) {
    user = await prisma.user.create({
      data: {
        username: XENIUM_USER.username,
        email: XENIUM_USER.email,
        name: XENIUM_USER.fullname || XENIUM_USER.username,
        cas_id: XENIUM_USER.cas_id || XENIUM_USER.username,
        is_deleted: false,
        metadata: withDatasetOrigin({ source: 'xenium_system_user' }),
      },
    });
    logger.info(`[XENIUM][seed_constants] created system user ${XENIUM_USER.username}`);
  } else {
    logger.info(`[XENIUM][seed_constants] system user already exists (${XENIUM_USER.username})`);
  }

  const adminRole = await prisma.role.findFirst({ where: { name: 'admin' } });
  if (!adminRole) {
    throw new Error('Role "admin" was not found after ensureRolesExist');
  }

  const existingRoleLink = await prisma.user_role.findUnique({
    where: {
      user_id_role_id: {
        user_id: user.id,
        role_id: adminRole.id,
      },
    },
  });

  if (!existingRoleLink) {
    await prisma.user_role.create({
      data: {
        user_id: user.id,
        role_id: adminRole.id,
      },
    });
  }

  return user.id;
}

/**
 * Seed analysis types from Xenium datasets' file types.
 * Xenium DATA_PRODUCTs typically have a single analysis type (xenium run output).
 *
 * @param {PrismaClient} prisma
 * @returns {Promise<void>}
 */
async function seedAnalysisTypes(prisma) {
  const defaultAnalysisTypes = [
    { name: 'WEB_SUMMARY', extension: '.html' },
    { name: 'XENIUM_ANALYSIS_SUMMARY', extension: '.html' },
    { name: 'XENIUM_ZIP', extension: '.zip' },
    { name: 'XENIUM_TEXT', extension: '.txt' },
  ];

  let created = 0;
  for (const analysisType of defaultAnalysisTypes) {
    // eslint-disable-next-line no-await-in-loop
    const existing = await prisma.analysis_type.findFirst({
      where: {
        name: { equals: analysisType.name, mode: 'insensitive' },
        extension: { equals: analysisType.extension, mode: 'insensitive' },
      },
    });

    if (!existing) {
      // eslint-disable-next-line no-await-in-loop
      await prisma.analysis_type.create({
        data: {
          ...analysisType,
          metadata: withDatasetOrigin(),
        },
      });
      created += 1;
    }
  }

  logger.info(`[XENIUM][seed_constants] seedAnalysisTypes complete (${created} created)`);
}

/**
 * Seed import source records for the Xenium instrument paths.
 *
 * @param {PrismaClient} prisma
 * @returns {Promise<void>}
 */
async function seedImportSources(prisma) {
  let created = 0;
  let updated = 0;

  for (const source of XENIUM_IMPORT_SOURCES) {
    // eslint-disable-next-line no-await-in-loop
    const existing = await prisma.import_source.findUnique({ where: { path: source.path } });
    if (!existing) {
      // eslint-disable-next-line no-await-in-loop
      await prisma.import_source.create({
        data: {
          ...source,
          metadata: withDatasetOrigin(),
        },
      });
      created += 1;
      continue;
    }

    const needsUpdate = existing.label !== source.label
      || existing.description !== source.description
      || existing.sort_order !== source.sort_order;
    if (needsUpdate) {
      // eslint-disable-next-line no-await-in-loop
      await prisma.import_source.update({
        where: { path: source.path },
        data: {
          label: source.label,
          description: source.description,
          sort_order: source.sort_order,
          metadata: withDatasetOrigin(existing.metadata),
        },
      });
      updated += 1;
    }
  }

  logger.info(`[XENIUM][seed_constants] seedImportSources complete (${created} created, ${updated} updated)`);
}

async function seedConstants(prisma) {
  logger.info('[XENIUM][seed_constants] Seeding prerequisites...');
  await ensureRolesExist(prisma);
  await createXeniumUser(prisma);
  await seedAnalysisTypes(prisma);
  await seedImportSources(prisma);
  logger.info('[XENIUM][seed_constants] Completed');
}

module.exports = {
  seedConstants,
  ensureRolesExist,
  createXeniumUser,
  seedAnalysisTypes,
  seedImportSources,
};

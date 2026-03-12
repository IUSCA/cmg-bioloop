/**
 * Legacy Migration Service — Backward Compatibility Re-Export
 *
 * This file exists so existing code that imports '@/services/legacyMigration'
 * continues to work without changes. It re-exports the CMG migration service,
 * which is the original implementation this file contained.
 *
 * For new code, import from the source-specific submodules:
 *   CMG:    '@/services/legacyMigration/cmg'
 *   Xenium: '@/services/legacyMigration/xenium'
 *   Both:   '@/services/legacyMigration'  (returns { cmg, xenium })
 */

const cmg = require('./legacyMigration/cmg');
const xenium = require('./legacyMigration/xenium');

module.exports = {
  ...cmg,
  cmg,
  xenium,
};

/**
 * Legacy Migration Service — Backward Compatibility Re-Export (UI)
 *
 * This file exists so existing components importing '@/services/legacyMigration'
 * or '../services/legacyMigration' continue to work without changes.
 * It re-exports the CMG migration service, which is the original implementation.
 *
 * For new code, import from the source-specific submodules:
 *   CMG:    '@/services/legacyMigration/cmg'
 *   Xenium: '@/services/legacyMigration/xenium'
 */

export {
  getDatasetMigrationStatus,
  getSessionMigrationStatus,
  isLegacyDataset,
  isLegacySession,
  isLegacyProject,
  isLegacyUser,
  needsHydration,
  isSessionHydrated,
  isMigrationInProgress,
} from "./legacyMigration/cmg/index.js";

export { default } from "./legacyMigration/cmg/index.js";

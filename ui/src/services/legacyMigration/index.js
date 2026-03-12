/**
 * Legacy Migration Services — Aggregated Re-Export (UI)
 *
 * Provides access to both CMG and Xenium migration services from a single import.
 * Existing code importing '../services/legacyMigration' resolves here via the
 * backward-compat shim at that path.
 *
 * Usage:
 *   import { cmg, xenium } from '@/services/legacyMigration';
 *
 * Or source-specific:
 *   import cmgService from '@/services/legacyMigration/cmg';
 *   import xeniumService from '@/services/legacyMigration/xenium';
 */

export { default as cmg } from "./cmg/index.js";
export { default as xenium } from "./xenium/index.js";

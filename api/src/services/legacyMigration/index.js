/**
 * Legacy Migration Services — Aggregated Re-Export
 *
 * Provides access to both CMG and Xenium migration services from a single import.
 * Consumers that need both can import from this file; consumers that need only one
 * source should import directly from the source-specific submodule.
 *
 * Backward compatibility:
 *   Existing code importing '@/services/legacyMigration' is re-directed here by
 *   updating that file to re-export from this module.
 */

const cmg = require('./cmg');
const xenium = require('./xenium');

module.exports = { cmg, xenium };

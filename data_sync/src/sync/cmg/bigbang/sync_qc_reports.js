const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');
const config = require('config');
const logger = require('../../../logger');

/**
 * Copy a directory tree, following symlinks (copies actual content, not symlinks).
 */
async function copyDirRecursive(src, dest) {
  await fsp.mkdir(dest, { recursive: true });

  const entries = await fsp.readdir(src, { withFileTypes: true });
  for (const entry of entries) {
    const srcPath = path.join(src, entry.name);
    const destPath = path.join(dest, entry.name);

    // Resolve symlinks to determine real type
    const stat = await fsp.stat(srcPath);
    if (stat.isDirectory()) {
      await copyDirRecursive(srcPath, destPath);
    } else {
      await fsp.copyFile(srcPath, destPath);
    }
  }
}

/**
 * Copy legacy QC/MultiQC reports from /N/project/... to a scratch-based
 * unified location so they can be mounted in the API container.
 *
 * Source structure (legacy CMG):
 *   <sourceDir>/<conversion_cmg_id>/<dataproduct_name>/
 *     *_fastqc.html, *_fastqc.zip, multiqc_report.html, multiqc_data/
 *
 * Target structure:
 *   <targetDir>/<conversion_cmg_id>/<dataproduct_name>/
 *     (same files, symlinks resolved to real content)
 *
 * The conversion_cmg_id directory names are preserved as-is so the
 * API can look them up via conversion.cmg_id.
 */
async function syncQcReports(prisma) {
  logger.info('[QC REPORTS] Starting legacy QC reports copy...');

  const sourceDir = config.get('cmg.legacyQcReportsDir');
  const targetDir = config.get('cmg.qcReportsTargetDir');

  if (!sourceDir || !targetDir) {
    logger.error('[QC REPORTS] legacyQcReportsDir or qcReportsTargetDir not configured');
    logger.error('[QC REPORTS] Set CMG_LEGACY_QC_REPORTS_DIR and CMG_QC_REPORTS_TARGET_DIR');
    throw new Error('QC reports paths not configured');
  }

  logger.info(`[QC REPORTS] Source: ${sourceDir}`);
  logger.info(`[QC REPORTS] Target: ${targetDir}`);

  // Check if source directory is accessible
  try {
    await fsp.access(sourceDir);
  } catch {
    logger.warn(`[QC REPORTS] Source directory not accessible: ${sourceDir}`);
    logger.info('[QC REPORTS] This is expected in non-production environments. Skipping.');
    return { copied: 0, skipped: 0, errors: [] };
  }

  // Ensure target directory exists
  await fsp.mkdir(targetDir, { recursive: true });

  const stats = { copied: 0, skipped: 0, errors: [] };

  // Iterate conversion_id directories
  let conversionDirs;
  try {
    conversionDirs = await fsp.readdir(sourceDir, { withFileTypes: true });
  } catch (error) {
    logger.error(`[QC REPORTS] Failed to read source directory: ${error.message}`);
    throw error;
  }

  const dirEntries = conversionDirs.filter((e) => e.isDirectory() || e.isSymbolicLink());
  logger.info(`[QC REPORTS] Found ${dirEntries.length} conversion directories to process`);

  for (const convEntry of dirEntries) {
    const convId = convEntry.name;
    const convSourcePath = path.join(sourceDir, convId);

    // Resolve symlinks at the conversion level to check if it's a real directory
    let convStat;
    try {
      convStat = await fsp.stat(convSourcePath);
    } catch (error) {
      logger.warn(`[QC REPORTS] Cannot stat ${convSourcePath}: ${error.message}`);
      stats.errors.push({ conversionId: convId, error: error.message });
      continue;
    }

    if (!convStat.isDirectory()) {
      continue;
    }

    // Iterate data product directories inside this conversion
    let productEntries;
    try {
      productEntries = await fsp.readdir(convSourcePath, { withFileTypes: true });
    } catch (error) {
      logger.warn(`[QC REPORTS] Cannot read ${convSourcePath}: ${error.message}`);
      stats.errors.push({ conversionId: convId, error: error.message });
      continue;
    }

    for (const prodEntry of productEntries) {
      const prodName = prodEntry.name;
      const prodSourcePath = path.join(convSourcePath, prodName);
      const prodTargetPath = path.join(targetDir, convId, prodName);

      // Skip if target already exists (idempotent)
      try {
        await fsp.access(prodTargetPath);
        stats.skipped += 1;
        continue;
      } catch {
        // Target doesn't exist, proceed with copy
      }

      try {
        await copyDirRecursive(prodSourcePath, prodTargetPath);
        stats.copied += 1;
      } catch (error) {
        logger.warn(
          `[QC REPORTS] Failed to copy ${convId}/${prodName}: ${error.message}`,
        );
        stats.errors.push({
          conversionId: convId,
          dataProduct: prodName,
          error: error.message,
        });
      }
    }
  }

  logger.info('');
  logger.info('='.repeat(80));
  logger.info('[QC REPORTS] Legacy QC reports copy complete');
  logger.info('='.repeat(80));
  logger.info(`Copied: ${stats.copied}`);
  logger.info(`Skipped (already existed): ${stats.skipped}`);
  if (stats.errors.length > 0) {
    logger.warn(`Errors: ${stats.errors.length}`);
    const show = stats.errors.slice(0, 10);
    show.forEach((e) => logger.warn(`  - ${JSON.stringify(e)}`));
    if (stats.errors.length > 10) {
      logger.warn(`  ... and ${stats.errors.length - 10} more`);
    }
  }
  logger.info('='.repeat(80));
  logger.info('');

  return stats;
}

module.exports = { syncQcReports };

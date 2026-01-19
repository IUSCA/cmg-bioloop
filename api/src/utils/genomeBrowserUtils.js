const {
  FILE_ROLES, INDEX_FORMATS, PRIMARY_TRACK_FORMATS, INDEX_TYPES_BY_MAIN_FORMAT,
} = require('@/constants');

/**
 * Normalize file format from file path
 * @param {string} path - File path
 * @returns {string|null} Normalized format string (e.g., 'BAM', 'VCF_GZ') or null
 */
function normalizeFormatFromPath(path) {
  if (!path) return null;

  const lower = path.toLowerCase();

  // Alignment formats with indexes
  if (lower.endsWith('.bam')) return 'BAM';
  if (lower.endsWith('.bam.bai')) return 'BAI';
  if (lower.endsWith('.bam.crai')) return 'CRAI';

  if (lower.endsWith('.cram')) return 'CRAM';
  if (lower.endsWith('.cram.crai')) return 'CRAI';

  // Variant formats with indexes
  if (lower.endsWith('.vcf.gz')) return 'VCF_GZ';
  if (lower.endsWith('.vcf.gz.tbi')) return 'TBI';
  if (lower.endsWith('.vcf.gz.csi')) return 'CSI';

  // BED formats with indexes
  if (lower.endsWith('.bed.gz')) return 'BED_GZ';
  if (lower.endsWith('.bed.gz.tbi')) return 'TBI';
  if (lower.endsWith('.bed.gz.csi')) return 'CSI';

  // GFF/GTF formats with indexes
  if (lower.endsWith('.gff.gz')) return 'GFF_GZ';
  if (lower.endsWith('.gff.gz.tbi')) return 'TBI';
  if (lower.endsWith('.gff.gz.csi')) return 'CSI';

  if (lower.endsWith('.gtf.gz')) return 'GTF_GZ';
  if (lower.endsWith('.gtf.gz.tbi')) return 'TBI';
  if (lower.endsWith('.gtf.gz.csi')) return 'CSI';

  // 10x ATAC fragments
  if (lower.endsWith('fragments.tsv.gz')) return 'FRAGMENTS_TSV_GZ';
  if (lower.endsWith('fragments.tsv.gz.tbi')) return 'TBI';

  // TSV with indexes (general case, check after fragments.tsv.gz)
  if (lower.endsWith('.tsv.gz') && !lower.endsWith('fragments.tsv.gz')) return 'TSV_GZ';
  if (lower.endsWith('.tsv.gz.tbi') && !lower.endsWith('fragments.tsv.gz.tbi')) return 'TBI';
  if (lower.endsWith('.tsv.gz.csi')) return 'CSI';

  // Big binary formats (self-indexed)
  if (lower.endsWith('.bw') || lower.endsWith('.bigwig')) return 'BIGWIG';
  if (lower.endsWith('.bb') || lower.endsWith('.bigbed')) return 'BIGBED';

  // FASTQ formats
  if (lower.endsWith('.fastq') || lower.endsWith('.fq')) return 'FASTQ';
  if (lower.endsWith('.fastq.gz') || lower.endsWith('.fq.gz')) return 'FASTQ_GZ';

  // Other common formats
  if (lower.endsWith('.tsv') && !lower.endsWith('.tsv.gz')) return 'TSV';
  if (lower.endsWith('.csv')) return 'CSV';
  if (lower.endsWith('.json')) return 'JSON';
  if (lower.endsWith('.log')) return 'LOG';
  if (lower.endsWith('.html')) return 'HTML';
  if (lower.endsWith('.pdf')) return 'PDF';
  if (lower.endsWith('.md5')) return 'CHECKSUM';

  return null;
}

/**
 * Get initial role from format
 * @param {string|null} format - Normalized format string
 * @returns {string|null} Role ('PRIMARY', 'INDEX') or null
 */
function getRoleFromFormat(format) {
  if (!format) return null;

  // Index formats by nature
  if (INDEX_FORMATS.includes(format)) return FILE_ROLES.INDEX;

  // Known track-capable main formats
  if (PRIMARY_TRACK_FORMATS.includes(format)) return FILE_ROLES.PRIMARY;

  // Others: no role
  return null;
}

/**
 * Get base name for index matching
 * Strips index suffixes to match primary files with their indexes
 * @param {string} path - File path
 * @param {string|null} format - Normalized format
 * @returns {string} Base filename for matching
 */
function baseNameForIndexMatching(path, format) {
  const filename = path.split(/[\\/]/).pop() || path;
  if (!format) return filename;

  // For index formats, strip their suffix to get main filename
  if (['BAI', 'CRAI'].includes(format)) {
    // sample.bam.bai → sample.bam
    return filename.replace(/\.bam\.(bai|crai)$/i, '.bam').replace(/\.cram\.crai$/i, '.cram');
  }

  if (['TBI', 'CSI'].includes(format)) {
    // sample.vcf.gz.tbi → sample.vcf.gz
    // sample.bed.gz.tbi → sample.bed.gz
    // fragments.tsv.gz.tbi → fragments.tsv.gz
    return filename
      .replace(/\.vcf\.gz\.(tbi|csi)$/i, '.vcf.gz')
      .replace(/\.bed\.gz\.(tbi|csi)$/i, '.bed.gz')
      .replace(/\.gff\.gz\.(tbi|csi)$/i, '.gff.gz')
      .replace(/\.gtf\.gz\.(tbi|csi)$/i, '.gtf.gz')
      .replace(/\.tsv\.gz\.(tbi|csi)$/i, '.tsv.gz');
  }

  // For primary files, filename *is* the base
  return filename;
}

/**
 * Find index file for a primary file
 * @param {Array} datasetFiles - Array of dataset_file objects
 * @param {Object} primaryFile - Primary dataset_file object
 * @param {string} primaryFile.path - Path of the primary file
 * @param {Object} primaryFile.metadata - Metadata object
 * @param {string} primaryFile.metadata.format - Format of the primary file
 * @returns {Object|null} Index dataset_file object or null
 */
function findIndexFileForPrimary(datasetFiles, primaryFile) {
  const primaryFormat = primaryFile.metadata?.format;
  if (!primaryFormat) return null;

  const expectedIndexFormats = INDEX_TYPES_BY_MAIN_FORMAT[primaryFormat];
  if (!expectedIndexFormats || expectedIndexFormats.length === 0) return null;

  const primaryBaseName = baseNameForIndexMatching(primaryFile.path, primaryFormat);
  const primaryDir = primaryFile.path.substring(0, primaryFile.path.lastIndexOf('/') + 1);

  // Look for index file in same directory with matching base name
  const indexFile = datasetFiles.find((file) => {
    const fileFormat = file.metadata?.format;
    if (!fileFormat || !expectedIndexFormats.includes(fileFormat)) return false;

    const fileDir = file.path.substring(0, file.path.lastIndexOf('/') + 1);
    if (fileDir !== primaryDir) return false;

    const fileBaseName = baseNameForIndexMatching(file.path, fileFormat);
    return fileBaseName === primaryBaseName;
  });

  return indexFile || null;
}

module.exports = {
  normalizeFormatFromPath,
  getRoleFromFormat,
  baseNameForIndexMatching,
  findIndexFileForPrimary,
};

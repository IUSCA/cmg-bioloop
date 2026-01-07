/**
 * CMG Sync - Constants
 * 
 * Constants for seeding roles, programs, conversion definitions, and arguments
 * Based on db_conversion/src/convert/constants/common.py
 */

/**
 * Bioloop Roles
 */
const BIOLOOP_ROLES = [
  { name: 'user', description: 'Basic user' },
  { name: 'operator', description: 'Can manage datasets' },
  { name: 'admin', description: 'Full system access' }
];

/**
 * CMG to Bioloop Role Mapping
 */
const ROLE_MAPPING = {
  'admin': 'operator',
  'god': 'admin',
  'user': 'user',
  'guest': 'user'
};

/**
 * CMG User (Synthetic user for system operations)
 */
const CMG_USER = {
  username: 'cmguser',
  email: 'cmguser@system.local',
  fullname: 'CMG System User',
  cas_id: 'cmguser',
  active: true,
  roles: ['admin'],
  createdDate: new Date()
};

/**
 * Command Line Programs
 */
const CMD_LINE_PROGRAMS = [
  {
    name: 'bcl2fastq',
    executable_path: '/opt/sca/data/conversion/bcl2fastq/bin/bcl2fastq',
    executable_directory: '',
    allow_additional_args: true
  },
  {
    name: 'bcl-convert',
    executable_path: '/opt/sca/data/conversion/bcl-convert/bin/bcl-convert',
    executable_directory: '',
    allow_additional_args: true
  },
  {
    name: 'cellranger-v8.0.1',
    executable_path: '/opt/sca/data/conversion/cellranger-v8.0.1/bin/cellranger-v8.0.1',
    executable_directory: '',
    allow_additional_args: true
  },
  {
    name: 'cellranger-v6.1.2',
    executable_path: '/opt/sca/data/conversion/cellranger-v6.1.2/bin/cellranger-v6.1.2',
    executable_directory: '',
    allow_additional_args: true
  },
  {
    name: 'cellranger-v4.0.0',
    executable_path: '/opt/sca/data/conversion/cellranger-v4.0.0/bin/cellranger-v4.0.0',
    executable_directory: '',
    allow_additional_args: true
  },
  {
    name: 'cellranger-arc',
    executable_path: '/opt/sca/data/conversion/cellranger-arc/bin/cellranger-arc',
    executable_directory: '',
    allow_additional_args: true
  },
  {
    name: 'cellranger-arc-v2',
    executable_path: '/opt/sca/data/conversion/cellranger-arc-v2/bin/cellranger-arc-v2',
    executable_directory: '',
    allow_additional_args: true
  },
  {
    name: 'cellranger-atac',
    executable_path: '/opt/sca/data/conversion/cellranger-atac/bin/cellranger-atac',
    executable_directory: '',
    allow_additional_args: true
  },
  {
    name: 'spaceranger-v3.0.1',
    executable_path: '/opt/sca/data/conversion/spaceranger-v3.0.1/bin/spaceranger-v3.0.1',
    executable_directory: '',
    allow_additional_args: true
  },
  {
    name: 'spaceranger-v1.3.1',
    executable_path: '/opt/sca/data/conversion/spaceranger-v1.3.1/bin/spaceranger-v1.3.1',
    executable_directory: '',
    allow_additional_args: true
  },
  {
    name: 'spaceranger-v1.1.0',
    executable_path: '/opt/sca/data/conversion/spaceranger-v1.1.0/bin/spaceranger-v1.1.0',
    executable_directory: '',
    allow_additional_args: true
  },
  {
    name: 'cellranger',
    executable_path: '/opt/sca/data/conversion/cellranger/bin/cellranger',
    executable_directory: '',
    allow_additional_args: true
  },
  {
    name: 'spaceranger',
    executable_path: '/opt/sca/data/conversion/spaceranger/bin/spaceranger',
    executable_directory: '',
    allow_additional_args: true
  }
];

/**
 * Conversion Definitions
 */
const CONVERSION_DEFINITIONS = [
  {
    name: 'bcl2fastq',
    description: 'Convert BCL files to FASTQ format for Illumina sequencing data',
    enabled: true,
    dataset_types: ['RAW_DATA'],
    tags: [],
    capture_logs: true,
    output_directory: '/opt/sca/data/conversion/output'
  },
  {
    name: 'bcl-convert',
    description: 'Convert BCL files to FASTQ format for Illumina sequencing data',
    enabled: true,
    dataset_types: ['RAW_DATA'],
    tags: [],
    capture_logs: true,
    output_directory: '/opt/sca/data/conversion/output'
  },
  {
    name: 'cellranger-v8.0.1',
    description: 'Single-cell RNA sequencing analysis pipeline for 10x Genomics data',
    enabled: true,
    dataset_types: ['RAW_DATA'],
    tags: [],
    capture_logs: true,
    output_directory: '/opt/sca/data/conversion/output'
  },
  {
    name: 'cellranger-v6.1.2',
    description: 'Single-cell RNA sequencing analysis pipeline for 10x Genomics data',
    enabled: true,
    dataset_types: ['RAW_DATA'],
    tags: [],
    capture_logs: true,
    output_directory: '/opt/sca/data/conversion/output'
  },
  {
    name: 'cellranger-v4.0.0',
    description: 'Single-cell RNA sequencing analysis pipeline for 10x Genomics data',
    enabled: true,
    dataset_types: ['RAW_DATA'],
    tags: [],
    capture_logs: true,
    output_directory: '/opt/sca/data/conversion/output'
  },
  {
    name: 'cellranger-arc',
    description: 'Single-cell multiome analysis pipeline for RNA+ATAC sequencing data',
    enabled: true,
    dataset_types: ['RAW_DATA'],
    tags: [],
    capture_logs: true,
    output_directory: '/opt/sca/data/conversion/output'
  },
  {
    name: 'cellranger-arc-v2',
    description: 'Single-cell multiome analysis pipeline for RNA+ATAC sequencing data',
    enabled: true,
    dataset_types: ['RAW_DATA'],
    tags: [],
    capture_logs: true,
    output_directory: '/opt/sca/data/conversion/output'
  },
  {
    name: 'cellranger-atac',
    description: 'Single-cell ATAC-seq analysis pipeline for chromatin accessibility data',
    enabled: true,
    dataset_types: ['RAW_DATA'],
    tags: [],
    capture_logs: true,
    output_directory: '/opt/sca/data/conversion/output'
  },
  {
    name: 'spaceranger-v3.0.1',
    description: 'Spatial transcriptomics analysis pipeline for tissue imaging data',
    enabled: true,
    dataset_types: ['RAW_DATA'],
    tags: [],
    capture_logs: true,
    output_directory: '/opt/sca/data/conversion/output'
  },
  {
    name: 'spaceranger-v1.3.1',
    description: 'Spatial transcriptomics analysis pipeline for tissue imaging data',
    enabled: true,
    dataset_types: ['RAW_DATA'],
    tags: [],
    capture_logs: true,
    output_directory: '/opt/sca/data/conversion/output'
  },
  {
    name: 'spaceranger-v1.1.0',
    description: 'Spatial transcriptomics analysis pipeline for tissue imaging data',
    enabled: true,
    dataset_types: ['RAW_DATA'],
    tags: [],
    capture_logs: true,
    output_directory: '/opt/sca/data/conversion/output'
  },
  {
    name: 'cellranger',
    description: 'Single-cell RNA sequencing analysis pipeline for 10x Genomics data (generic version)',
    enabled: true,
    dataset_types: ['RAW_DATA'],
    tags: [],
    capture_logs: true,
    output_directory: '/opt/sca/data/conversion/output'
  },
  {
    name: 'spaceranger',
    description: 'Spatial transcriptomics analysis pipeline for tissue imaging data (generic version)',
    enabled: true,
    dataset_types: ['RAW_DATA'],
    tags: [],
    capture_logs: true,
    output_directory: '/opt/sca/data/conversion/output'
  }
];

/**
 * Argument Data
 */
const ARGUMENT_DATA = [
  {
    name: '--no-lane-splitting',
    value_type: 'BOOLEAN',
    allowed_values: [],
    is_required: false,
    default_value: null,
    is_flag: true,
    description: 'Do not split lanes into separate files',
    min_value: null,
    max_value: null,
    min_length: null,
    max_length: null,
    position: null,
    dynamic_variable_name: null,
    program_names: null  // Will be set during seeding - all programs
  },
  {
    name: '--ignore-missing-bcls',
    value_type: 'BOOLEAN',
    allowed_values: [],
    is_required: false,
    default_value: null,
    is_flag: true,
    description: 'Ignore missing bcls',
    min_value: null,
    max_value: null,
    min_length: null,
    max_length: null,
    position: null,
    dynamic_variable_name: null,
    program_names: ['bcl2fastq']  // bcl2fastq only
  },
  {
    name: '--ignore-missing-filter',
    value_type: 'BOOLEAN',
    allowed_values: [],
    is_required: false,
    default_value: null,
    is_flag: true,
    description: 'Ignore missing filter',
    min_value: null,
    max_value: null,
    min_length: null,
    max_length: null,
    position: null,
    dynamic_variable_name: null,
    program_names: null  // All programs
  },
  {
    name: '--ignore-missing-positions',
    value_type: 'BOOLEAN',
    allowed_values: [],
    is_required: false,
    default_value: null,
    is_flag: true,
    description: 'Ignore missing positions',
    min_value: null,
    max_value: null,
    min_length: null,
    max_length: null,
    position: null,
    dynamic_variable_name: null,
    program_names: null  // All programs
  },
  {
    name: '--delete-undetermined',
    value_type: 'BOOLEAN',
    allowed_values: [],
    is_required: false,
    default_value: null,
    is_flag: true,
    description: 'Delete undetermined reads',
    min_value: null,
    max_value: null,
    min_length: null,
    max_length: null,
    position: null,
    dynamic_variable_name: null,
    program_names: null  // All programs
  },
  {
    name: '--filter-single-index',
    value_type: 'BOOLEAN',
    allowed_values: [],
    is_required: false,
    default_value: null,
    is_flag: true,
    description: 'Filter single index reads',
    min_value: null,
    max_value: null,
    min_length: null,
    max_length: null,
    position: null,
    dynamic_variable_name: null,
    program_names: null  // All programs
  },
  {
    name: '--barcode-mismatches',
    value_type: 'NUMBER',
    allowed_values: ['0', '1', '2'],
    is_required: false,
    default_value: null,
    is_flag: false,
    description: 'Number of mismatches allowed for barcode',
    min_value: null,
    max_value: null,
    min_length: null,
    max_length: null,
    position: null,
    dynamic_variable_name: null,
    program_names: ['bcl2fastq']  // bcl2fastq only
  },
  {
    name: '--uses-bases-mask',
    value_type: 'BOOLEAN',
    allowed_values: [],
    is_required: false,
    default_value: null,
    is_flag: true,
    description: 'Uses bases mask',
    min_value: null,
    max_value: null,
    min_length: null,
    max_length: null,
    position: null,
    dynamic_variable_name: null,
    program_names: ['bcl2fastq']  // bcl2fastq only
  },
  {
    name: '--sample-sheet',
    value_type: 'STRING',
    allowed_values: [],
    is_required: false,
    default_value: null,
    is_flag: false,
    description: 'Sample sheet',
    min_value: null,
    max_value: null,
    min_length: null,
    max_length: 1000,
    position: null,
    dynamic_variable_name: null,
    program_names: ['bcl2fastq']  // bcl2fastq only
  }
];

/**
 * Poller Names
 */
const POLLER_NAMES = {
  USER_ROLES: 'user_roles',
  PROJECT_ACL: 'project_acl',
  DATASET_ACTIVITY: 'dataset_activity',
  DATASET_METADATA: 'dataset_metadata',
  WORKFLOW_STATUS: 'workflow_status'
};

module.exports = {
  BIOLOOP_ROLES,
  ROLE_MAPPING,
  CMG_USER,
  CMD_LINE_PROGRAMS,
  CONVERSION_DEFINITIONS,
  ARGUMENT_DATA,
  POLLER_NAMES
};


const INCLUDE_STATES = {
  states: {
    select: {
      state: true,
      timestamp: true,
      metadata: true,
    },
    orderBy: {
      timestamp: 'desc',
    },
  },
};

const INCLUDE_FILES = {
  files: {
    select: {
      path: true,
      md5: true,
      name: true,
    },
    where: {
      NOT: {
        filetype: 'directory',
      },
    },
  },
};

const INCLUDE_WORKFLOWS = {
  workflows: {
    select: {
      id: true,
    },
  },
};

const INCLUDE_AUDIT_LOGS = {
  audit_logs: {
    include: {
      user: {
        include: {
          user_role: {
            select: { roles: true },
          },
        },
      },
    },
    orderBy: {
      timestamp: 'desc',
    },
  },
};

const INCLUDE_PROJECTS = {
  projects: {
    select: {
      project: {
        select: {
          id: true,
          name: true,
          slug: true,
          description: true,
          browser_enabled: true,
          funding: true,
          metadata: true,
          created_at: true,
          updated_at: true,
          owner_id: true,
        },
      },
    },
  },
};

const INCLUDE_DATASET_UPLOAD_LOG_RELATIONS = {
  dataset: {
    select: {
      id: true,
      name: true,
      type: true,
      metadata: true,
      origin_path: true,
      create_method: true,
      created_at: true,
      source_datasets: {
        select: {
          source_dataset: true,
        },
      },
      projects: {
        select: {
          project: true,
        },
      },
      genomic_details: {
        select: {
          genome_type: true,
          genome_value: true,
        },
      },
      analysis_type: true,
      audit_logs: {
        where: {
          action: 'create',
        },
        select: {
          user: true,
          timestamp: true,
        },
        orderBy: {
          timestamp: 'asc',
        },
        take: 1,
      },
    },
  },
};

const DATASET_CREATE_METHODS = {
  UPLOAD: 'UPLOAD',
  IMPORT: 'IMPORT',
  SCAN: 'SCAN',
};

const DONE_STATUSES = ['REVOKED', 'FAILURE', 'SUCCESS'];

const UPLOAD_STATUSES = {
  UPLOADING: 'UPLOADING',
  UPLOAD_FAILED: 'UPLOAD_FAILED',
  UPLOADED: 'UPLOADED',
  VERIFYING: 'VERIFYING',
  VERIFIED: 'VERIFIED',
  VERIFICATION_FAILED: 'VERIFICATION_FAILED',
  PROCESSING: 'PROCESSING',
  PROCESSING_FAILED: 'PROCESSING_FAILED',
  COMPLETE: 'COMPLETE',
  PERMANENTLY_FAILED: 'PERMANENTLY_FAILED',
};

const DATA_REQUEST_STATUS = {
  PENDING: 'PENDING',
  COMPLETE: 'COMPLETE',
};

const WORKFLOWS = {
  INTEGRATED: 'integrated',
  INTAKE_INTEGRATED: 'intake_integrated',
  STAGE: 'stage',
  STAGE_MIGRATED: 'stage_migrated',
  STAGE_MIGRATED_XENIUM: 'stage_migrated_xenium',
  HYDRATE_SESSION: 'hydrate_session',
};

const DATASET_STATES = {
  READY: 'READY',
  ARCHIVED: 'ARCHIVED',
  REGISTERED: 'REGISTERED',
  FETCHED: 'FETCHED',
  STAGED: 'STAGED',
  DELETED: 'DELETED',
  // Legacy migration states
  MIGRATION_INITIATED: 'MIGRATION_INITIATED',
  RETRIEVED: 'RETRIEVED',
  INSPECTED: 'INSPECTED',
  METADATA_POPULATED: 'METADATA_POPULATED',
  MIGRATED: 'MIGRATED',
};

const auth = {
  verify: {
    response: {
      status: {
        SUCCESS: 'success',
        SIGNUP_REQUIRED: 'signup_required',
        NOT_A_USER: 'not_a_user',
      },
    },
  },
};

const ALERT_STATUSES = {
  SCHEDULED: 'SCHEDULED',
  ACTIVE: 'ACTIVE',
  EXPIRED: 'EXPIRED',
};

const ALERT_TYPES = {
  INFO: 'INFO',
  WARNING: 'WARNING',
  ERROR: 'ERROR',
};

// Genome Browser - File Roles
const FILE_ROLES = {
  PRIMARY: 'PRIMARY',
  INDEX: 'INDEX',
};

// Genome Browser - Index types by main format
const INDEX_TYPES_BY_MAIN_FORMAT = {
  // Alignment
  BAM: ['BAI', 'CRAI'],
  CRAM: ['CRAI'],

  // Variants
  VCF_GZ: ['TBI', 'CSI'],

  // Tabix-indexed tabular
  BED_GZ: ['TBI', 'CSI'],
  GFF_GZ: ['TBI', 'CSI'],
  GTF_GZ: ['TBI', 'CSI'],
  TSV_GZ: ['TBI', 'CSI'],

  // 10x ATAC
  FRAGMENTS_TSV_GZ: ['TBI'],

  // Big binary formats (self-indexed)
  BIGWIG: [],
  BIGBED: [],

  // Others: no sidecar index
  FASTQ: [],
  FASTQ_GZ: [],
  CSV: [],
  TSV: [],
  JSON: [],
  HTML: [],
  PDF: [],
  LOG: [],
  CHECKSUM: [],
};

// Genome Browser - Formats that are index files
const INDEX_FORMATS = ['BAI', 'CRAI', 'TBI', 'CSI'];

// Genome Browser - Track-capable primary formats
const PRIMARY_TRACK_FORMATS = [
  'BAM',
  'CRAM',
  'VCF_GZ',
  'BED_GZ',
  'BIGWIG',
  'BIGBED',
  'FRAGMENTS_TSV_GZ',
];

// Genome Browser - Browser-compatible formats (for filtering in track selection)
const BROWSER_COMPATIBLE_FORMATS = [
  'BAM',
  'CRAM',
  'VCF_GZ',
  'BIGWIG',
  'BIGBED',
  'FRAGMENTS_TSV_GZ',
  'BED_GZ',
];

const NOTIFICATION_TYPES = {
  DATASET_CREATED: 'DATASET_CREATED',
};

module.exports = {
  INCLUDE_FILES,
  INCLUDE_STATES,
  INCLUDE_WORKFLOWS,
  INCLUDE_AUDIT_LOGS,
  INCLUDE_DATASET_UPLOAD_LOG_RELATIONS,
  auth,
  DONE_STATUSES,
  DATASET_CREATE_METHODS,
  UPLOAD_STATUSES,
  WORKFLOWS,
  ALERT_TYPES,
  ALERT_STATUSES,
  DATASET_STATES,
  INCLUDE_PROJECTS,
  FILE_ROLES,
  INDEX_TYPES_BY_MAIN_FORMAT,
  INDEX_FORMATS,
  PRIMARY_TRACK_FORMATS,
  BROWSER_COMPATIBLE_FORMATS,
  DATA_REQUEST_STATUS,
  NOTIFICATION_TYPES,
};

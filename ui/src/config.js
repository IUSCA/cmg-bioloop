const exports = {
  mode: 'development',
  // vite server redirects traffic on URLs starting with apiBaseURL
  // to http://${config.apiHost}:${config.apiPort} in dev environment
  apiBasePath: '/api',
  uploadApiBasePath: import.meta.env.VITE_UPLOAD_API_BASE_PATH || 'https://localhost',
  casReturn: import.meta.env.VITE_CAS_RETURN || 'https://localhost/auth/iucas',
  googleReturn: import.meta.env.VITE_GOOGLE_RETURN || 'https://localhost/auth/google',
  cilogonReturn: import.meta.env.VITE_CILOGON_RETURN || 'https://localhost/auth/cil',
  microsoftReturn: import.meta.env.VITE_MICROSOFT_RETURN || 'https://localhost/auth/microsoft',
  refreshTokenTMinusSeconds: {
    appToken: 300,
    uploadToken: 20,
  },
  analyticsId: 'G-FOO',
  appTitle: 'CMG',
  contact: {
    app_admin: 'bioloop-ops-l@list.iu.edu',
  },
  dataset_polling_interval: 10000,
  paths: {
    download: '',
  },
  file_browser: {
    cache_busting_id: 'fe09b01', // any random string different from the previous value will work
  },
  enable_delete_archive: true,
  dataset: {
    types: {
      RAW_DATA: {
        key: 'RAW_DATA',
        label: 'Raw Data',
        collection_path: 'rawdata',
        icon: 'mdi-dna',
      },
      DATA_PRODUCT: {
        key: 'DATA_PRODUCT',
        label: 'Data Product',
        collection_path: 'dataproducts',
        icon: 'mdi-package-variant-closed',
      },
    },
  },
  download_types: {
    SLATE_SCRATCH: 'SLATE_SCRATCH',
    BROWSER: 'BROWSER',
  },
  metric_measurements: {
    SDA: 'sda',
    SLATE_SCRATCH: '/N/scratch',
    SLATE_SCRATCH_FILES: '/N/scratch files',
    SLATE_PROJECT: '/N/project',
  },
  auth_enabled: {
    google: true,
    cilogon: true,
    microsoft: true,
  },
  dashboard: {
    active_tasks: {
      steps: [
        // Integrated workflow
        'await stability',
        'inspect',
        'archive',
        'stage',
        'validate',
        'setup_download',
        'delete source',
        // Stage_migrated workflow (additional steps)
        'begin_migration',
        'retrieve_archive',
        'populate_metadata',
        'end_migration',
        // Genomic conversion workflow
        'convert',
        'generate qc',
        'copy reports',
        'derive data products',
      ],
      refresh_interval_ms: 10000,
    },
  },
  alertForEnvironments: ['ci'],
  enabledFeatures: {
    genomeBrowser: true,
    notifications: {
      enabledForRoles: [],
    },
    import: {
      enabledForRoles: ['admin'],
    },
    downloads: true,
    signup: true,
    uploads: {
      enabledForRoles: ['admin'],
    },
    upload_verify_checksums: true, // Enable BLAKE3 manifest-based checksum verification for uploads
  },
  notifications: {
    pollingInterval: 5000, // milliseconds
  },
  filesystem_search_spaces: [
    {
      slateScratch: {
        base_path: import.meta.env.VITE_SCRATCH_BASE_DIR || '/bioloop/scratch/space',
        mount_path: import.meta.env.VITE_SCRATCH_MOUNT_DIR || '/bioloop/user/scratch/mount/dir',
        key: 'slateScratch',
        label: 'Slate-Scratch',
      },
    },
    {
      slateProject: {
        base_path: import.meta.env.VITE_PROJECT_BASE_DIR || '/bioloop/project/space',
        mount_path: import.meta.env.VITE_PROJECT_MOUNT_DIR || '/bioloop/user/project/mount/dir',
        key: 'slateProject',
        label: 'Slate-Project',
      },
    },
  ],
  trackFileTypes: [
    { name: 'BAM', id: 'bam' },
    { name: 'BigWig', id: 'bigwig' },
    { name: 'VCF', id: 'vcf' },
    { name: 'FASTQ', id: 'fastq' },
  ],
  fileTypeOptions: [
    { text: 'BAM', value: 'bam' },
    { text: 'BigWig', value: 'bigwig' },
    { text: 'VCF', value: 'vcf' },
    { text: 'BED', value: 'bed' },
    { text: 'GTF', value: 'gtf' },
    { text: 'FASTQ', value: 'fastq' },
    { text: 'FASTA', value: 'fasta' },
  ],
  restricted_import_dirs: {
    slateScratch: {
      paths: import.meta.env.VITE_SCRATCH_IMPORT_RESTRICTED_DIRS || '/scratch/space/restricted',
      key: 'scratch',
    },
    slateProject: {
      paths: import.meta.env.VITE_PROJECT_IMPORT_RESTRICTED_DIRS || '/project/space/restricted',
      key: 'project',
    },
  },
  upload: {
    scope_prefix: 'upload_file:',
  },
  conversion: {
    allow_multiple_dataset_conversions: false, // Set to false by default to allow only single dataset conversions
  },
  genomic_conversion_programs: [
    'bcl2fastq',
    'bcl-convert',
    'cellranger-v8.0.1',
    'cellranger-v6.1.2',
    'cellranger-v4.0.0',
    'cellranger-arc',
    'cellranger-arc-v2',
    'cellranger-atac',
    'spaceranger-v3.0.1',
    'spaceranger-v1.3.1',
    'spaceranger-v1.1.0',
  ],
  genomeBrowserUrl:
    import.meta.env.VITE_GENOME_BROWSER_URL || 'https://epigenomegateway.wustl.edu/browser',
  legacy: {
    enabled: true,
  }
};

export default exports;

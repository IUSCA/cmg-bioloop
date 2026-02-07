const exports = {
  sidebar: {
    user_items: [
      {
        icon: 'mdi-flask',
        title: 'Projects',
        path: '/projects',
        test_id: 'sidebar-projects',
      },
      {
        icon: 'mdi-folder-plus-outline',
        title: 'Create Dataset',
        test_id: 'sidebar-create-dataset',
        children: [
          {
            feature_key: 'import',
            icon: 'mdi-file-cog-outline',
            title: 'Import',
            path: '/datasets/imports',
          },
          {
            feature_key: 'uploads',
            icon: 'mdi:folder-upload',
            title: 'Upload',
            path: '/datasetUpload',
          },
        ],
      },
    ],
    operator_items: [
      {
        icon: 'mdi-monitor-dashboard',
        title: 'Dashboard',
        path: '/dashboard',
        test_id: 'sidebar-dashboard',
      },
      // {
      //   icon: "mdi-file-lock",
      //   title: "Data Products",
      //   path: "/dataproducts",
      // },
      {
        icon: 'mdi-transition',
        title: 'Conversions',
        path: '/conversions',
        test_id: 'sidebar-conversions',
      },
      // {
      //   icon: "mdi-folder-upload",
      //   title: "Data Uploader",
      //   path: "/datauploader",  // cspell: disable-line
      // },
      {
        icon: 'mdi-dna',
        title: 'Raw Data',
        path: '/rawdata',
        test_id: 'sidebar-raw-data',
      },
      {
        icon: 'mdi-package-variant-closed',
        title: 'Data Products',
        path: '/dataproducts',
        test_id: 'sidebar-data-products',
      },
      {
        icon: 'mdi-chart-gantt',
        title: 'Tracks',
        path: '/tracks',
        test_id: 'sidebar-tracks',
      },
      {
        icon: 'mdi-eye',
        title: 'Sessions',
        path: '/sessions',
        test_id: 'sidebar-sessions',
      },
      {
        icon: 'mdi-table-account',
        title: 'User Management',
        path: '/users',
        test_id: 'sidebar-user-management',
      },
      {
        icon: 'mdi-format-list-bulleted',
        title: 'Stats/Tracking',
        path: '/stats',
        test_id: 'sidebar-stats-tracking',
      },
      {
        icon: 'mdi:map-marker-path',
        title: 'Workflows',
        path: '/workflows',
        test_id: 'sidebar-workflows',
      },
      // {
      //   icon: "mdi-account-multiple",
      //   title: "Group Management",
      //   path: "/groups",
      // },
      // {
      //   icon: 'mdi-delete-empty-outline',
      //   title: 'Data Cleanup',
      //   path: '/clean',
      // },
    ],
    bottom_items: [
      {
        icon: 'mdi-information',
        title: 'About',
        path: '/about',
        test_id: 'sidebar-about',
      },
      {
        icon: 'mdi-account-details',
        title: 'Profile',
        path: '/profile',
        test_id: 'sidebar-profile',
      },
      {
        icon: 'mdi-logout-variant',
        title: 'Logout',
        path: '/auth/logout',
        test_id: 'sidebar-logout',
      },
    ],
    admin_items: [],
  },
  UPLOAD_STATUSES: {
    // Statuses that only appear in the UI
    UNINITIATED: 'UNINITIATED',
    COMPUTING_CHECKSUMS: 'COMPUTING_CHECKSUMS',
    CHECKSUM_COMPUTATION_FAILED: 'CHECKSUM_COMPUTATION_FAILED',
    // Statuses that appear in the UI and are also persisted to the database
    UPLOADING: 'UPLOADING',
    UPLOAD_FAILED: 'UPLOAD_FAILED',
    UPLOADED: 'UPLOADED',
    VERIFYING: 'VERIFYING', // Integrity verification in progress (async Celery task)
    VERIFIED: 'VERIFIED', // Integrity verified, ready to trigger workflow
    VERIFICATION_FAILED: 'VERIFICATION_FAILED', // Integrity check failed before workflow
    PROCESSING: 'PROCESSING',
    PROCESSING_FAILED: 'PROCESSING_FAILED',
    COMPLETE: 'COMPLETE',
    PERMANENTLY_FAILED: 'PERMANENTLY_FAILED', // Max retries exceeded
  },
  DATASET_CREATE_METHODS: {
    UPLOAD: 'UPLOAD',
    IMPORT: 'IMPORT',
    SCAN: 'SCAN',
  },
  auth: {
    verify: {
      response: {
        status: {
          SUCCESS: 'success',
          SIGNUP_REQUIRED: 'signup_required',
          NOT_A_USER: 'not_a_user',
        },
      },
    },
  },
  GENOME_TYPES: {
    // Primates
    human: {
      label: 'Human',
      image: 'https://epigenomegateway.wustl.edu/browser/images/Human.png',
      genomes: ['hg19', 'hg38', 't2t-chm13-v1.1'],
    },
    chimp: {
      label: 'Chimp',
      image: 'https://epigenomegateway.wustl.edu/browser/images/Chimp.png',
      genomes: ['panTro6', 'panTro5', 'panTro4'],
    },
    gorilla: {
      label: 'Gorilla',
      image: 'https://vizhub.wustl.edu/public/gorGor3/Gorilla.png',
      genomes: ['gorGor4', 'gorGor3'],
    },
    gibbon: {
      label: 'Gibbon',
      image: 'https://vizhub.wustl.edu/public/nomLeu3/Gibbon.png',
      genomes: ['nomLeu3'],
    },
    baboon: {
      label: 'Baboon',
      image: 'https://vizhub.wustl.edu/public/papAnu2/Baboon.png',
      genomes: ['papAnu2'],
    },
    rhesus: {
      label: 'Rhesus',
      image: 'https://vizhub.wustl.edu/public/rheMac8/Rhesus_macaque.png',
      genomes: ['rheMac8', 'rheMac3', 'rheMac2'],
    },
    marmoset: {
      label: 'Marmoset',
      image: 'https://vizhub.wustl.edu/public/calJac3/Marmoset.png',
      genomes: ['calJac3'],
    },
    // Mammals
    mouse: {
      label: 'Mouse',
      image: 'https://epigenomegateway.wustl.edu/browser/images/Mouse.png',
      genomes: ['mm39', 'mm10', 'mm9'],
    },
    rat: {
      label: 'Rat',
      image: 'https://epigenomegateway.wustl.edu/browser/images/Rat.png',
      genomes: ['rn6', 'rn4'],
    },
    cow: {
      label: 'Cow',
      image: 'https://vizhub.wustl.edu/public/bosTau8/Cow.png',
      genomes: ['bosTau8'],
    },
    rabbit: {
      label: 'Rabbit',
      image: 'https://vizhub.wustl.edu/public/oryCun2/rabbit.png',
      genomes: ['oryCun2'],
    },
    dog: {
      label: 'Dog',
      image: 'https://vizhub.wustl.edu/public/canFam3/dog.png',
      genomes: ['canFam3', 'canFam2'],
    },
    opossum: {
      label: 'Opossum',
      image: 'https://vizhub.wustl.edu/public/monDom5/opossum.png',
      genomes: ['monDom5'],
    },
    // Birds
    chicken: {
      label: 'Chicken',
      image: 'https://epigenomegateway.wustl.edu/browser/images/Chicken.png',
      genomes: ['galGal6', 'galGal5'],
    },
    // Fish
    zebrafish: {
      label: 'Zebrafish',
      image: 'https://epigenomegateway.wustl.edu/browser/images/Zebrafish.png',
      genomes: ['danRer11', 'danRer10', 'danRer7'],
    },
    'spotted gar': {
      label: 'Spotted Gar',
      image: 'https://vizhub.wustl.edu/public/lepOcu1/SpottedGar.png',
      genomes: ['lepOcu1'],
    },
    // Invertebrates
    'fruit fly': {
      label: 'Fruit Fly',
      image: 'https://epigenomegateway.wustl.edu/legacy/images/Fruit%20fly.png',
      genomes: ['dm6'],
    },
    'c.elegans': {
      label: 'C. elegans',
      image: 'https://epigenomegateway.wustl.edu/legacy/images/C.elegans.png',
      genomes: ['ce11'],
    },
    seahare: {
      label: 'Seahare',
      image: 'https://vizhub.wustl.edu/public/aplCal3/seaHare.png',
      genomes: ['aplCal3'],
    },
    // Plants
    arabidopsis: {
      label: 'Arabidopsis',
      image: 'https://epigenomegateway.wustl.edu/browser/images/Arabidopsis.png',
      genomes: ['araTha1'],
    },
    'green algae': {
      label: 'Green Algae',
      image: 'https://vizhub.wustl.edu/public/Creinhardtii506/Creinhardtii506.png',
      genomes: ['Creinhardtii5.6'],
    },
    // Microorganisms
    yeast: {
      label: 'Yeast',
      image: 'https://epigenomegateway.wustl.edu/browser/images/Yeast.png',
      genomes: ['sacCer3'],
    },
    'p. falciparum': {
      label: 'P. falciparum',
      image: 'https://epigenomegateway.wustl.edu/browser/images/Pfalciparum.png',
      genomes: ['Pfal3D7'],
    },
    trypanosome: {
      label: 'Trypanosome',
      image: 'https://vizhub.wustl.edu/public/trypanosome/trypanosome.png',
      genomes: ['TbruceiTREU927', 'TbruceiLister427'],
    },
    // Viruses
    virus: {
      label: 'Virus',
      image: 'https://vizhub.wustl.edu/public/virus/virus.png',
      genomes: ['SARS-CoV-2', 'MERS', 'SARS', 'Ebola', 'hpv16'],
    },
  },
  genomeBrowser: {
    // Browser types
    browserTypes: {
      IGV: 'igv',
      WASHU: 'washu',
    },
    // Browser display names
    browserLabels: {
      igv: 'IGV Browser',
      washu: 'WashU Epigenome Browser',
    },
    // Browser modal titles
    browserTitles: {
      igv: 'IGV Genome Browser',
      washu: 'WashU Epigenome Browser',
      default: 'Genome Browser',
    },
    // Default browser selection
    defaultBrowser: 'igv',
    // File roles
    fileRoles: {
      PRIMARY: 'PRIMARY',
      INDEX: 'INDEX',
    },
    // Browser-compatible formats (for display/filtering)
    browserCompatibleFormats: [
      'BAM',
      'CRAM',
      'VCF_GZ',
      'BIGWIG',
      'BIGBED',
      'FRAGMENTS_TSV_GZ',
      'BED_GZ',
    ],
    // Index formats
    indexFormats: ['BAI', 'CRAI', 'TBI', 'CSI'],
    // Primary track formats
    primaryTrackFormats: [
      'BAM',
      'CRAM',
      'VCF_GZ',
      'BED_GZ',
      'BIGWIG',
      'BIGBED',
      'FRAGMENTS_TSV_GZ',
    ],
  },
  dataRequestStatus: {
    PENDING: 'PENDING',
    COMPLETE: 'COMPLETE',
  },
  artifact_type: {
    JOB_SCRIPT: 'JOB_SCRIPT',
    ENVIRONMENT_SETUP: 'ENVIRONMENT_SETUP',
    RUNTIME_CONFIG: 'RUNTIME_CONFIG',
    RESOURCE_MANIFEST: 'RESOURCE_MANIFEST',
    SECRETS: 'SECRETS',
    DEPENDENCY_FILE: 'DEPENDENCY_FILE',
    CUSTOM: 'CUSTOM',
  },
  storage_type: {
    INLINE: 'INLINE',
    FILE_PATH: 'FILE_PATH',
    EXTERNAL_URL: 'EXTERNAL_URL',
  },
};

export default exports;

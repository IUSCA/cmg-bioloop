app_id = 'cmg-test.sca.iu.edu'

role_mapping = {
  'admin': 'operator',
  'god': 'admin',
  'user': 'user',
  'guest': 'user'
}

"""
Arguments data for seeding conversion pipeline definitions.
"""

ARGUMENT_DATA = [
    {
        'name': '--no-lane-splitting',
        'value_type': 'BOOLEAN',
        'allowed_values': [],
        'is_required': False,
        'default_value': None,
        'is_flag': True,
        'description': 'Do not split lanes into separate files',
        'min_value': None,
        'max_value': None,
        'min_length': None,
        'max_length': None,
        'position': None,
        'dynamic_variable_name': None,
        'program_id': None,  # Will be set during seeding - links to all programs
    },
    {
        'name': '--ignore-missing-bcls',
        'value_type': 'BOOLEAN',
        'allowed_values': [],
        'is_required': False,
        'default_value': None,
        'is_flag': True,
        'description': 'Ignore missing bcls',
        'min_value': None,
        'max_value': None,
        'min_length': None,
        'max_length': None,
        'position': None,
        'dynamic_variable_name': None,
        'program_id': None,  # Will be set during seeding - links to bcl2fastq only
    },
    {
        'name': '--ignore-missing-filter',
        'value_type': 'BOOLEAN',
        'allowed_values': [],
        'is_required': False,
        'default_value': None,
        'is_flag': True,
        'description': 'Ignore missing filter',
        'min_value': None,
        'max_value': None,
        'min_length': None,
        'max_length': None,
        'position': None,
        'dynamic_variable_name': None,
        'program_id': None,  # Will be set during seeding - links to all programs
    },
    {
        'name': '--ignore-missing-positions',
        'value_type': 'BOOLEAN',
        'allowed_values': [],
        'is_required': False,
        'default_value': None,
        'is_flag': True,
        'description': 'Ignore missing positions',
        'min_value': None,
        'max_value': None,
        'min_length': None,
        'max_length': None,
        'position': None,
        'dynamic_variable_name': None,
        'program_id': None,  # Will be set during seeding - links to all programs
    },
    {
        'name': '--delete-undetermined',
        'value_type': 'BOOLEAN',
        'allowed_values': [],
        'is_required': False,
        'default_value': None,
        'is_flag': True,
        'description': 'Delete undetermined reads',
        'min_value': None,
        'max_value': None,
        'min_length': None,
        'max_length': None,
        'position': None,
        'dynamic_variable_name': None,
        'program_id': None,  # Will be set during seeding - links to all programs
    },
    {
        'name': '--filter-single-index',
        'value_type': 'BOOLEAN',
        'allowed_values': [],
        'is_required': False,
        'default_value': None,
        'is_flag': True,
        'description': 'Filter single index reads',
        'min_value': None,
        'max_value': None,
        'min_length': None,
        'max_length': None,
        'position': None,
        'dynamic_variable_name': None,
        'program_id': None,  # Will be set during seeding - links to all programs
    },
    {
        'name': '--barcode-mismatches',
        'value_type': 'NUMBER',
        'allowed_values': ['0', '1', '2'],
        'is_required': False,
        'default_value': None,
        'is_flag': False,
        'description': 'Number of mismatches allowed for barcode',
        'min_value': None,
        'max_value': None,
        'min_length': None,
        'max_length': None,
        'position': None,
        'dynamic_variable_name': None,
        'program_id': None,  # Will be set during seeding - links to bcl2fastq only
    },
    {
        'name': '--uses-bases-mask',
        'value_type': 'BOOLEAN',
        'allowed_values': [],
        'is_required': False,
        'default_value': None,
        'is_flag': True,
        'description': 'Uses bases mask',
        'min_value': None,
        'max_value': None,
        'min_length': None,
        'max_length': None,
        'position': None,
        'dynamic_variable_name': None,
        'program_id': None,  # Will be set during seeding - links to bcl2fastq only
    },
    {
        'name': '--sample-sheet',
        'value_type': 'STRING',
        'allowed_values': [],
        'is_required': False,
        'default_value': None,
        'is_flag': False,
        'description': 'Sample sheet',
        'min_value': None,
        'max_value': None,
        'min_length': None,
        'max_length': 1000,
        'position': None,
        'dynamic_variable_name': None,
        'program_id': None,  # Will be set during seeding - links to bcl2fastq only
    },
]

"""
Command line programs data for seeding conversion pipeline definitions.
Equivalent to api/prisma/seed_data/cmd_line_programs.js
"""

CMD_LINE_PROGRAMS = [
    {
        'name': 'bcl2fastq',
        'executable_path': '/opt/sca/data/conversion/bcl2fastq/bin/bcl2fastq',
        'executable_directory': '',
        'allow_additional_args': True,
    },
    {
        'name': 'bcl-convert',
        'executable_path': '/opt/sca/data/conversion/bcl-convert/bin/bcl-convert',
        'executable_directory': '',
        'allow_additional_args': True,
    },
    {
        'name': 'cellranger-v8.0.1',
        'executable_path': '/opt/sca/data/conversion/cellranger-v8.0.1/bin/cellranger-v8.0.1',
        'executable_directory': '',
        'allow_additional_args': True,
    },
    {
        'name': 'cellranger-v6.1.2',
        'executable_path': '/opt/sca/data/conversion/cellranger-v6.1.2/bin/cellranger-v6.1.2',
        'executable_directory': '',
        'allow_additional_args': True,
    },
    {
        'name': 'cellranger-v4.0.0',
        'executable_path': '/opt/sca/data/conversion/cellranger-v4.0.0/bin/cellranger-v4.0.0',
        'executable_directory': '',
        'allow_additional_args': True,
    },
    {
        'name': 'cellranger-arc',
        'executable_path': '/opt/sca/data/conversion/cellranger-arc/bin/cellranger-arc',
        'executable_directory': '',
        'allow_additional_args': True,
    },
    {
        'name': 'cellranger-arc-v2',
        'executable_path': '/opt/sca/data/conversion/cellranger-arc-v2/bin/cellranger-arc-v2',
        'executable_directory': '',
        'allow_additional_args': True,
    },
    {
        'name': 'cellranger-atac',
        'executable_path': '/opt/sca/data/conversion/cellranger-atac/bin/cellranger-atac',
        'executable_directory': '',
        'allow_additional_args': True,
    },
    {
        'name': 'spaceranger-v3.0.1',
        'executable_path': '/opt/sca/data/conversion/spaceranger-v3.0.1/bin/spaceranger-v3.0.1',
        'executable_directory': '',
        'allow_additional_args': True,
    },
    {
        'name': 'spaceranger-v1.3.1',
        'executable_path': '/opt/sca/data/conversion/spaceranger-v1.3.1/bin/spaceranger-v1.3.1',
        'executable_directory': '',
        'allow_additional_args': True,
    },
    {
        'name': 'spaceranger-v1.1.0',
        'executable_path': '/opt/sca/data/conversion/spaceranger-v1.1.0/bin/spaceranger-v1.1.0',
        'executable_directory': '',
        'allow_additional_args': True,
    },
    {
        'name': 'cellranger',
        'executable_path': '/opt/sca/data/conversion/cellranger/bin/cellranger',
        'executable_directory': '',
        'allow_additional_args': True,
    },
    {
        'name': 'spaceranger',
        'executable_path': '/opt/sca/data/conversion/spaceranger/bin/spaceranger',
        'executable_directory': '',
        'allow_additional_args': True,
    },
]

# Other program names (excluding bcl2fastq) - matches seed.js lines 358-362
OTHER_PROGRAM_NAMES = [
    'bcl-convert', 'cellranger-v8.0.1', 'cellranger-v6.1.2', 'cellranger-v4.0.0',
    'cellranger-arc', 'cellranger-arc-v2', 'cellranger-atac', 'spaceranger-v3.0.1',
    'spaceranger-v1.3.1', 'spaceranger-v1.1.0', 'cellranger', 'spaceranger'
]

"""
Conversion definitions data for seeding conversion pipeline definitions.
Equivalent to api/prisma/seed_data/conversion_definitions.js
"""

CONVERSION_DEFINITIONS = [
    {
        'name': 'bcl2fastq',
        'description': 'Convert BCL files to FASTQ format for Illumina sequencing data',
        'enabled': True,
        'dataset_types': ['RAW_DATA'],
        'tags': [],
        'capture_logs': True,
        'output_directory': '/opt/sca/data/conversion/output',
    },
    {
        'name': 'bcl-convert',
        'description': 'Convert BCL files to FASTQ format for Illumina sequencing data',
        'enabled': True,
        'dataset_types': ['RAW_DATA'],
        'tags': [],
        'capture_logs': True,
        'output_directory': '/opt/sca/data/conversion/output',
    },
    {
        'name': 'cellranger-v8.0.1',
        'description': 'Single-cell RNA sequencing analysis pipeline for 10x Genomics data',
        'enabled': True,
        'dataset_types': ['RAW_DATA'],
        'tags': [],
        'capture_logs': True,
        'output_directory': '/opt/sca/data/conversion/output',
    },
    {
        'name': 'cellranger-v6.1.2',
        'description': 'Single-cell RNA sequencing analysis pipeline for 10x Genomics data',
        'enabled': True,
        'dataset_types': ['RAW_DATA'],
        'tags': [],
        'capture_logs': True,
        'output_directory': '/opt/sca/data/conversion/output',
    },
    {
        'name': 'cellranger-v4.0.0',
        'description': 'Single-cell RNA sequencing analysis pipeline for 10x Genomics data',
        'enabled': True,
        'dataset_types': ['RAW_DATA'],
        'tags': [],
        'capture_logs': True,
        'output_directory': '/opt/sca/data/conversion/output',
    },
    {
        'name': 'cellranger-arc',
        'description': 'Single-cell multiome analysis pipeline for RNA+ATAC sequencing data',
        'enabled': True,
        'dataset_types': ['RAW_DATA'],
        'tags': [],
        'capture_logs': True,
        'output_directory': '/opt/sca/data/conversion/output',
    },
    {
        'name': 'cellranger-arc-v2',
        'description': 'Single-cell multiome analysis pipeline for RNA+ATAC sequencing data',
        'enabled': True,
        'dataset_types': ['RAW_DATA'],
        'tags': [],
        'capture_logs': True,
        'output_directory': '/opt/sca/data/conversion/output',
    },
    {
        'name': 'cellranger-atac',
        'description': 'Single-cell ATAC-seq analysis pipeline for chromatin accessibility data',
        'enabled': True,
        'dataset_types': ['RAW_DATA'],
        'tags': [],
        'capture_logs': True,
        'output_directory': '/opt/sca/data/conversion/output',
    },
    {
        'name': 'spaceranger-v3.0.1',
        'description': 'Spatial transcriptomics analysis pipeline for tissue imaging data',
        'enabled': True,
        'dataset_types': ['RAW_DATA'],
        'tags': [],
        'capture_logs': True,
        'output_directory': '/opt/sca/data/conversion/output',
    },
    {
        'name': 'spaceranger-v1.3.1',
        'description': 'Spatial transcriptomics analysis pipeline for tissue imaging data',
        'enabled': True,
        'dataset_types': ['RAW_DATA'],
        'tags': [],
        'capture_logs': True,
        'output_directory': '/opt/sca/data/conversion/output',
    },
    {
        'name': 'spaceranger-v1.1.0',
        'description': 'Spatial transcriptomics analysis pipeline for tissue imaging data',
        'enabled': True,
        'dataset_types': ['RAW_DATA'],
        'tags': [],
        'capture_logs': True,
        'output_directory': '/opt/sca/data/conversion/output',
    },
    {
        'name': 'cellranger',
        'description': 'Single-cell RNA sequencing analysis pipeline for 10x Genomics data (generic version)',
        'enabled': True,
        'dataset_types': ['RAW_DATA'],
        'tags': [],
        'capture_logs': True,
        'output_directory': '/opt/sca/data/conversion/output',
    },
    {
        'name': 'spaceranger',
        'description': 'Spatial transcriptomics analysis pipeline for tissue imaging data (generic version)',
        'enabled': True,
        'dataset_types': ['RAW_DATA'],
        'tags': [],
        'capture_logs': True,
        'output_directory': '/opt/sca/data/conversion/output',
    },
]

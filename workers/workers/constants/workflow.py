WORKFLOWS = {
    'INTEGRATED': 'integrated',
    'STAGE': 'stage',
    'STAGE_MIGRATED': 'stage_migrated',
    'FILE_INFO_POPULATION': 'file_info_population',
    'HYDRATE_SESSION': 'hydrate_session'
}

WORKFLOW_FINISHED_STATUSES = {
    'REVOKED': 'REVOKED',
    'FAILURE': 'FAILURE',
    'SUCCESS': 'SUCCESS'
}

WORKFLOW_STATUSES = {
    'PENDING': 'PENDING',
    'STARTED': 'STARTED',
    **WORKFLOW_FINISHED_STATUSES
}

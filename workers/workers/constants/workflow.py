WORKFLOWS = {
    'INTEGRATED': 'integrated',
    'INTAKE_INTEGRATED': 'intake_integrated',
    'STAGE': 'stage',
    'STAGE_MIGRATED': 'stage_migrated',
    # Xenium-specific workflows
    'SUBDIR_WF_INITIATOR': 'subdir_wf_initiator',
    'STAGE_MIGRATED_XENIUM': 'stage_migrated_xenium',
    # Shared
    'HYDRATE_SESSION': 'hydrate_session',
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

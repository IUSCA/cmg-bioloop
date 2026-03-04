import api from "./api";

const DONE_STATUSES = ["REVOKED", "FAILURE", "SUCCESS"];

class WorkflowService {
  getWorkflowNames() {
    return api.get("/workflows/names");
  }

  getAll({
    last_task_run = false,
    prev_task_runs = false,
    workflow_id = null,
    dataset_id = null,
    dataset_name = null,
    workflow_name = null,
    status = null,
    skip = 0,
    limit = 10,
    initiator = false,
  } = {}) {
    return api.get("/workflows", {
      params: {
        last_task_run,
        prev_task_runs,
        status,
        workflow_id: workflow_id,
        skip,
        limit,
        dataset_id,
        dataset_name,
        workflow_name,
        initiator,
      },
      paramsSerializer: {
        // to create workflow_id=123&workflow_id=456
        // instead of workflow_id[]=123&workflow_id[]=456
        indexes: null, // by default: false
      },
    });
  }

  getById(id, last_task_runs = false, prev_task_runs = false) {
    return api.get(`/workflows/${id}`, {
      params: {
        last_task_runs,
        prev_task_runs,
      },
    });
  }

  pause(id) {
    return api.post(`/workflows/${id}/pause`);
  }

  delete(id) {
    return api.delete(`/workflows/${id}`);
  }

  resume(id) {
    return api.post(`/workflows/${id}/resume`);
  }

  is_workflow_done(workflow) {
    return DONE_STATUSES.includes(workflow?.status);
  }

  is_step_pending(step_name, workflows) {
    const active_wfs = (workflows || []).filter(
      (wf) => !this.is_workflow_done(wf),
    );
    const pending_steps = active_wfs
      .flatMap((wf) => wf.steps || [])
      .filter(
        (step) =>
          step &&
          step.name &&
          step.name.toLowerCase() === step_name.toLowerCase(),
      )
      .filter((step) => !DONE_STATUSES.includes(step.status));

    return pending_steps.length > 0;
  }

  is_staging_workflow_active(workflows) {
    // Check if any staging-related workflow is active (not done)
    // This includes: stage, stage_migrated, and integrated workflows
    const staging_workflow_names = ["stage", "stage_migrated", "integrated"];

    const active_staging_wfs = (workflows || []).filter(
      (wf) =>
        staging_workflow_names.includes(wf.name) && !this.is_workflow_done(wf),
    );

    return active_staging_wfs.length > 0;
  }

  get_integrated_workflow_status(workflows) {
    // Get the status of the integrated workflow specifically
    // Returns: 'SUCCESS', 'FAILURE', 'ACTIVE', or null
    // For Import Log table: Only SUCCESS is considered success, all other terminal states are failures
    const ACTIVE_STATES = ["PENDING", "STARTED"];

    const integratedWorkflows = (workflows || []).filter(
      (wf) => wf.name === "integrated",
    );

    if (integratedWorkflows.length === 0) {
      return null;
    }

    // Get the most recent integrated workflow (in case there are multiple)
    const latestWorkflow = integratedWorkflows[integratedWorkflows.length - 1];

    if (!latestWorkflow.status) {
      return null;
    }

    // Active: workflow is running (PENDING or STARTED)
    if (ACTIVE_STATES.includes(latestWorkflow.status)) {
      return "ACTIVE";
    }

    // Success: ONLY the SUCCESS status
    if (latestWorkflow.status === "SUCCESS") {
      return "SUCCESS";
    }

    // Failure: Any other status (FAILURE, REVOKED, EXCEPTION, etc.)
    return "FAILURE";
  }

  getWorkflowProcesses({
    workflow_id,
    step = null,
    task_id = null,
    pid = null,
  }) {
    return api.get(`/workflows/processes`, {
      params: {
        workflow_id,
        step,
        task_id,
        pid,
      },
    });
  }

  getLogs({ processId, beforeId = null, afterId = null, level = null }) {
    return api.get(`/workflows/processes/${processId}/logs`, {
      params: {
        before_id: beforeId,
        after_id: afterId,
        level,
      },
    });
  }

  getCountsByStatus() {
    return api.get("/workflows/counts_by_status");
  }
}

export default new WorkflowService();

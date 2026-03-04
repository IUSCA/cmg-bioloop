import constants from "@/constants";

/**
 * Resolves the subject of a workflow — i.e. the entity the workflow ran on —
 * and returns the display label, router link, and ID needed to render a link.
 *
 * Returns null if the workflow name is not recognised or the subject ID is
 * absent from the workflow object.
 *
 * @param {object} workflow - Workflow object as returned by GET /workflows
 * @returns {{ label: string, to: string, id: number|string }|null}
 */
function resolveWorkflowSubject(workflow) {
  const config = constants.WORKFLOW_SUBJECT_CONFIGS[workflow?.name];
  if (!config) return null;

  const subjectId = workflow?.[config.subjectKey];
  if (!subjectId) return null;

  return {
    label: config.label,
    to: config.getRoute(subjectId),
    id: subjectId,
  };
}

export default { resolveWorkflowSubject };

// SLURM Directives Configuration
// These are platform-specific directives for SLURM execution platform

export const SLURM_DIRECTIVES = {
  // sbatch directives
  sbatch: {
    // Resource allocation
    nodes: {
      name: '--nodes',
      type: 'number',
      description: 'Number of nodes to allocate',
      min: 1,
      max: 1000,
      default: 1,
    },
    ntasks: {
      name: '--ntasks',
      type: 'number', 
      description: 'Total number of tasks',
      min: 1,
      max: 10000,
      default: 1,
    },
    ntasksPerNode: {
      name: '--ntasks-per-node',
      type: 'number',
      description: 'Number of tasks per node',
      min: 1,
      max: 128,
      default: 1,
    },
    cpusPerTask: {
      name: '--cpus-per-task',
      type: 'number',
      description: 'Number of CPUs per task',
      min: 1,
      max: 128,
      default: 1,
    },
    mem: {
      name: '--mem',
      type: 'string',
      description: 'Memory per node (e.g., 4G, 8GB)',
      pattern: '^\\d+[KMGTPE]?$',
      placeholder: '4G',
    },
    memPerCpu: {
      name: '--mem-per-cpu',
      type: 'string',
      description: 'Memory per CPU (e.g., 2G)',
      pattern: '^\\d+[KMGTPE]?$',
      placeholder: '2G',
    },
    time: {
      name: '--time',
      type: 'string',
      description: 'Time limit (DD-HH:MM:SS)',
      pattern: '^\\d+-\\d{2}:\\d{2}:\\d{2}$',
      placeholder: '01-00:00:00',
    },
    
    // Job identification
    jobName: {
      name: '--job-name',
      type: 'string',
      description: 'Name for the job',
      maxLength: 100,
      placeholder: 'my-job',
    },
    
    // Partition and queue
    partition: {
      name: '--partition',
      type: 'select',
      description: 'SLURM partition to use',
      options: [
        { value: 'cpu', label: 'CPU' },
        { value: 'gpu', label: 'GPU' },
        { value: 'highmem', label: 'High Memory' },
        { value: 'interactive', label: 'Interactive' },
        { value: 'debug', label: 'Debug' },
      ],
    },
    
    // Output and logging
    output: {
      name: '--output',
      type: 'string',
      description: 'File for standard output',
      placeholder: 'job_%j.out',
      maxLength: 200,
    },
    error: {
      name: '--error',
      type: 'string', 
      description: 'File for standard error',
      placeholder: 'job_%j.err',
      maxLength: 200,
    },
    
    // Email notifications
    mailUser: {
      name: '--mail-user',
      type: 'email',
      description: 'Email address for notifications',
      placeholder: 'user@example.com',
    },
    mailType: {
      name: '--mail-type',
      type: 'multiselect',
      description: 'When to send email notifications',
      options: [
        { value: 'NONE', label: 'None' },
        { value: 'BEGIN', label: 'Begin' },
        { value: 'END', label: 'End' },
        { value: 'FAIL', label: 'Fail' },
        { value: 'REQUEUE', label: 'Requeue' },
        { value: 'ALL', label: 'All' },
      ],
    },
    
    // GPU support
    gpus: {
      name: '--gpus',
      type: 'number',
      description: 'Number of GPUs to allocate',
      min: 0,
      max: 8,
      default: 0,
    },
    gpusPerNode: {
      name: '--gpus-per-node',
      type: 'number',
      description: 'Number of GPUs per node',
      min: 0,
      max: 8,
      default: 0,
    },
    
    // Job dependencies
    dependency: {
      name: '--dependency',
      type: 'string',
      description: 'Job dependencies (e.g., afterok:12345)',
      placeholder: 'afterok:12345',
    },
    
    // Quality of service
    qos: {
      name: '--qos',
      type: 'select',
      description: 'Quality of service',
      options: [
        { value: 'normal', label: 'Normal' },
        { value: 'high', label: 'High' },
        { value: 'low', label: 'Low' },
      ],
    },
  },
  
  // srun directives (subset of sbatch, for interactive jobs)
  srun: {
    nodes: {
      name: '--nodes',
      type: 'number',
      description: 'Number of nodes to allocate',
      min: 1,
      max: 1000,
      default: 1,
    },
    ntasks: {
      name: '--ntasks',
      type: 'number',
      description: 'Total number of tasks',
      min: 1,
      max: 10000,
      default: 1,
    },
    cpusPerTask: {
      name: '--cpus-per-task',
      type: 'number',
      description: 'Number of CPUs per task',
      min: 1,
      max: 128,
      default: 1,
    },
    mem: {
      name: '--mem',
      type: 'string',
      description: 'Memory per node (e.g., 4G, 8GB)',
      pattern: '^\\d+[KMGTPE]?$',
      placeholder: '4G',
    },
    time: {
      name: '--time',
      type: 'string',
      description: 'Time limit (DD-HH:MM:SS)',
      pattern: '^\\d+-\\d{2}:\\d{2}:\\d{2}$',
      placeholder: '01-00:00:00',
    },
    partition: {
      name: '--partition',
      type: 'select',
      description: 'SLURM partition to use',
      options: [
        { value: 'cpu', label: 'CPU' },
        { value: 'gpu', label: 'GPU' },
        { value: 'highmem', label: 'High Memory' },
        { value: 'interactive', label: 'Interactive' },
        { value: 'debug', label: 'Debug' },
      ],
    },
    gpus: {
      name: '--gpus',
      type: 'number',
      description: 'Number of GPUs to allocate',
      min: 0,
      max: 8,
      default: 0,
    },
  },
};

// Helper function to get directives for a specific SLURM command
export function getSlurmDirectives(command = 'sbatch') {
  return SLURM_DIRECTIVES[command] || SLURM_DIRECTIVES.sbatch;
}

// Helper function to validate SLURM directive values
export function validateSlurmDirective(directive, value) {
  if (!directive || value === null || value === undefined || value === '') {
    return { valid: true }; // Empty values are valid (optional directives)
  }
  
  const { type, min, max, pattern, maxLength } = directive;
  
  // Type validation
  if (type === 'number') {
    const numValue = Number(value);
    if (isNaN(numValue)) {
      return { valid: false, error: 'Must be a valid number' };
    }
    if (min !== undefined && numValue < min) {
      return { valid: false, error: `Must be at least ${min}` };
    }
    if (max !== undefined && numValue > max) {
      return { valid: false, error: `Must be at most ${max}` };
    }
  }
  
  if (type === 'string' || type === 'email') {
    if (maxLength && value.length > maxLength) {
      return { valid: false, error: `Must be no more than ${maxLength} characters` };
    }
    if (pattern && !new RegExp(pattern).test(value)) {
      return { valid: false, error: 'Invalid format' };
    }
    if (type === 'email' && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)) {
      return { valid: false, error: 'Must be a valid email address' };
    }
  }
  
  return { valid: true };
}

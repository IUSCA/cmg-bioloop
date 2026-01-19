module.exports = [
  {
    script: 'src/index.js',
    name: 'api',
    exec_mode: 'cluster',
    instances: 2,
    exp_backoff_restart_delay: 100,
    max_restarts: 3,
    watch: false,
  },
  {
    script: 'src/scripts/cmg_poller_sync.js',
    name: 'cmg-poller',
    exec_mode: 'fork',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '1G',
    error_file: '/opt/sca/data/logs/cmg_poller_error.log',
    out_file: '/opt/sca/data/logs/cmg_poller_out.log',
    log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
  },
];

// https://pm2.keymetrics.io/docs/usage/application-declaration/
// Archive node ecosystem config - runs only celery_worker and watch
// Used on nodes where instruments deposit data directly (k2, k3, k4, nanopore)
// These nodes only handle await_stability, inspect, and archive tasks
module.exports = {
  apps: [
    {
      name: "celery_worker",
      script: "python",
      args: "-m celery -A workers.celery_app worker --loglevel INFO -O fair --pidfile celery_worker.pid --hostname 'cmg-bioloop-archive-w1@%h' --autoscale=1,1 --queues 'cmg-bioloop-archive.cmg-test.sca.iu.edu.q'",
      watch: false,
      interpreter: "",
      log_date_format: "YYYY-MM-DD HH:mm Z",
      error_file: "../logs/workers/celery_worker.err",
      out_file: "../logs/workers/celery_worker.log",
      kill_timeout: "10000",
      exp_backoff_restart_delay: 100,
      max_restarts: 3,
    },
    {
      name: "watch",
      script: "python",
      args: "-u -m workers.scripts.watch",
      watch: false,
      interpreter: "",
      log_date_format: "YYYY-MM-DD HH:mm Z",
      error_file: "../logs/workers/watch.err",
      out_file: "../logs/workers/watch.log",
      exp_backoff_restart_delay: 100,
      max_restarts: 3,
    }
  ]
}

#!/bin/bash

# This script is for development/testing - starts fetch worker
# For production, use fetch.ecosystem.config.js and archive.ecosystem.config.js
python -m celery \
  -A workers.fetch_celery_app worker \
  --loglevel INFO \
  -O fair \
  --pidfile celery_worker.pid \
  --hostname 'cmg-test-celery-fetch-w1@%h' \
  --autoscale 8,3 \
  --queues 'fetch.cmg-test.sca.iu.edu.q'
  # --detach

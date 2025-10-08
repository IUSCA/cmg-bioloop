#!/bin/bash

python -m celery \
  -A workers.celery_app worker \
  --loglevel INFO \
  -O fair \
  --pidfile celery_worker.pid \
  --hostname 'cmg-test-celery-w1@%h' \
  --autoscale 8,3 \
  --queues 'cmg-test.sca.iu.edu.q'
  # --detach

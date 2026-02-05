#!/bin/bash

python -m celery \
  -A workers.celery_app worker \
  --loglevel INFO \
  -O fair \
  --pidfile celery_worker.pid \
  --hostname 'cmg-test-celery-v2-w1@%h' \
  --autoscale 8,3 \
  --queues 'cmg-bioloop-v2.cmg-test.sca.iu.edu.q'
  # --detach

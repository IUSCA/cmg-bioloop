#!/bin/bash

# In single-node mode, listen to both fetch and archive queues
python -m celery \
  -A workers.celery_app worker \
  --loglevel INFO \
  -O fair \
  --pidfile celery_worker.pid \
  --hostname 'cmg-test-celery-w1@%h' \
  --autoscale 8,3 \
  --queues 'fetch.cmg-test.sca.iu.edu.q,archive.cmg-test.sca.iu.edu.q'
  # --detach

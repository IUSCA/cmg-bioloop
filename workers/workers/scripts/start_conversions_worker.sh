#!/bin/bash

python -m celery \
  -A workers.conversions_app worker \
  --loglevel INFO \
  -O fair \
  --pidfile conversions_worker.pid \
  --hostname 'cmg-test-celery-v2-conversions-w1@%h' \
  --autoscale=8,2 \
  --queues 'conversion-v2.cmg-test.sca.iu.edu.q'
  --statedb=conversions_worker.state.db
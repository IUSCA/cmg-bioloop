#!/bin/bash
set -e

echo "Running entrypoint script for workers container"

echo "Waiting for .env file to be ready..."
while [ ! -f ".env" ] || ! grep -Eq "^APP_API_TOKEN=[^ ]+" ".env"; do
  echo "Waiting for .env file to be ready and contain APP_API_TOKEN..."
  sleep 1
done

echo ".env file is ready and should contain APP_API_TOKEN"

echo "loading environment variables from .env file"
if [ -f .env ]; then
  echo ".env file exists"
  export $(grep -v '^#' .env | xargs)
  echo "exported environment variables from .env file"
fi

echo ".env file is ready. Starting the worker..."

# Install conversion pipelines if this is the conversion_worker
if [ "$WORKER_TYPE" = "conversion_worker" ]; then
  echo "Installing conversion pipelines..."
  
  CONVERSION_BASE="/opt/sca/data/conversion"
  
  # Install bcl2fastq
  if [ -f /usr/local/bin/bcl2fastq ]; then
    cp /usr/local/bin/bcl2fastq "$CONVERSION_BASE/bcl2fastq/bin/"
    chmod +x "$CONVERSION_BASE/bcl2fastq/bin/bcl2fastq"
    echo "✓ bcl2fastq"
  fi
  
  # Install bcl-convert
  if [ -f /usr/local/bin/bcl-convert ]; then
    cp /usr/local/bin/bcl-convert "$CONVERSION_BASE/bcl-convert/bin/"
    chmod +x "$CONVERSION_BASE/bcl-convert/bin/bcl-convert"
    echo "✓ bcl-convert"
  fi
  
  # Install cellranger versions
  for version in "8.0.1" "6.1.2" "4.0.0"; do
    if [ -d "/opt/cellranger-$version" ]; then
      cp -r /opt/cellranger-$version/* "$CONVERSION_BASE/cellranger-v$version/"
      chmod +x "$CONVERSION_BASE/cellranger-v$version/bin/cellranger" 2>/dev/null || true
      echo "✓ cellranger-v$version"
    fi
  done
  
  # Install cellranger-arc
  if [ -d "/opt/cellranger-arc-1.0.0" ]; then
    cp -r /opt/cellranger-arc-1.0.0/* "$CONVERSION_BASE/cellranger-arc/"
    chmod +x "$CONVERSION_BASE/cellranger-arc/bin/cellranger-arc" 2>/dev/null || true
    echo "✓ cellranger-arc"
  fi
  
  if [ -d "/opt/cellranger-arc-2.0.0" ]; then
    cp -r /opt/cellranger-arc-2.0.0/* "$CONVERSION_BASE/cellranger-arc-v2/"
    chmod +x "$CONVERSION_BASE/cellranger-arc-v2/bin/cellranger-arc" 2>/dev/null || true
    echo "✓ cellranger-arc-v2"
  fi
  
  # Install cellranger-atac
  if [ -d "/opt/cellranger-atac-1.2.0" ]; then
    cp -r /opt/cellranger-atac-1.2.0/* "$CONVERSION_BASE/cellranger-atac/"
    chmod +x "$CONVERSION_BASE/cellranger-atac/bin/cellranger-atac" 2>/dev/null || true
    echo "✓ cellranger-atac"
  fi
  
  # Install spaceranger versions
  for version in "3.0.1" "1.3.1" "1.1.0"; do
    if [ -d "/opt/spaceranger-$version" ]; then
      cp -r /opt/spaceranger-$version/* "$CONVERSION_BASE/spaceranger-v$version/"
      chmod +x "$CONVERSION_BASE/spaceranger-v$version/bin/spaceranger" 2>/dev/null || true
      echo "✓ spaceranger-v$version"
    fi
  done
  
  echo "Pipeline installation complete"
fi

# Remove stale PID files, if they exist
if [ "$WORKER_TYPE" = "celery_worker" ] && [ -f celery_worker.pid ]; then
  echo "Removing stale celery_worker.pid file"
  rm celery_worker.pid
fi

if [ "$WORKER_TYPE" = "conversion_worker" ] && [ -f conversion_worker.pid ]; then
  echo "Removing stale conversion_worker.pid file"
  rm conversion_worker.pid
fi

# Start the appropriate worker based on the container invoking this entrypoint script
if [ "$WORKER_TYPE" = "celery_worker" ]; then
  echo "Starting Celery Worker"
  
  # Start the upload polling job in the background (runs every 30 seconds)
  echo "Starting upload polling job in background..."
  (
    while true; do
      sleep 30
      echo "[$(date)] Running manage_upload_workflows..."
      python -u -m workers.scripts.manage_upload_workflows --dry-run=False --max-retries=3 2>&1 || true
    done
  ) &
  POLLING_PID=$!
  echo "Upload polling job started with PID: $POLLING_PID"
  
  exec python -m celery \
    -A workers.celery_app worker \
    --loglevel INFO \
    -O fair \
    --statedb celery_worker.state \
    --pidfile celery_worker.pid \
    --hostname 'cmg-test-celery-w1@%h' \
    --autoscale 8,3 \
    --queues 'cmg-test.sca.iu.edu.q'
      # --detach
elif [ "$WORKER_TYPE" = "conversion_worker" ]; then
  echo "Starting Conversion Worker"
  exec python -m celery \
    -A workers.conversions_app worker \
    --loglevel INFO \
    -O fair \
    --statedb conversion_worker.state \
    --pidfile conversion_worker.pid \
    --hostname 'cmg-test-celery-w1@%h' \
    --autoscale 8,3 \
    --queues 'conversion.cmg-test.sca.iu.edu.q'
      # --detach
elif [ "$WORKER_TYPE" = "watch" ]; then
  echo "Starting Watch Worker"
  python -m workers.scripts.watch
elif [ "$WORKER_TYPE" = "metrics" ]; then
  echo "Starting Metrics Worker"
  python -m workers.scripts.metrics
elif [ "$WORKER_TYPE" = "purge_staged_datasets" ]; then
  echo "Starting Purge Staged Datasets Worker"
  python -m workers.scripts.purge_staged_datasets
elif [ "$WORKER_TYPE" = "purge_stale_workflows" ]; then
  echo "Starting Purge Stale Workflows Worker"
  python -m workers.scripts.purge_stale_workflows
elif [ "$WORKER_TYPE" = "manage_pending_dataset_uploads" ]; then
  echo "Starting Manage Pending Dataset Uploads Worker"
  python -m workers.scripts.manage_pending_dataset_uploads
elif [ "$WORKER_TYPE" = "process_upload_dataset" ]; then
  echo "Starting Process Upload Dataset Worker"
  python -m workers.scripts.process_upload_dataset
else
  echo "Invalid Worker Type"
fi

echo "Completed entrypoint script for workers container"

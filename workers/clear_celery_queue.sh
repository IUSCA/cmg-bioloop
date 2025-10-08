#!/bin/bash

# Celery Queue Clear Script
# 
# This script clears all queues and purges all tasks from the Celery/RabbitMQ system.
# It should be run from within a worker container that has access to the Celery configuration.
#
# USAGE EXAMPLES:
# ===============
#
# 1. Run from celery_worker container (RECOMMENDED - only way that works):
#    docker exec -it cmg-bioloop-2-celery_worker-1 /opt/sca/app/clear_celery_queue.sh
#
# 2. Run from conversion_worker container:
#    docker exec -it cmg-bioloop-2-conversion_worker-1 /opt/sca/app/clear_celery_queue.sh
#
# 3. Run from watch container:
#    docker exec -it cmg-bioloop-2-watch-1 /opt/sca/app/clear_celery_queue.sh
#
# 4. Run interactively (if you want to inspect first):
#    docker exec -it cmg-bioloop-2-celery_worker-1 bash
#    # cd /opt/sca/app && ./clear_celery_queue.sh
#
# 5. Run with verbose output:
#    docker exec -it cmg-bioloop-2-celery_worker-1 bash -c "set -x; /opt/sca/app/clear_celery_queue.sh"
#
# PREREQUISITES:
# ==============
# - Worker containers must be running (celery_worker, conversion_worker, or watch)
# - Celery app must be accessible at workers.workers.celery_app
# - Script is available at /opt/sca/app/ in worker containers
# - Container names should match the pattern 'cmg-bioloop-2-*-1' (adjust if different)
#
# WHAT IT CLEARS:
# ===============
# - All Celery queues (uses Celery's purge command which clears all registered queues)
# - All pending tasks/messages in those queues
# - This includes: celery, cmg-test.sca.iu.edu.q, conversion.cmg-test.sca.iu.edu.q, etc.
# - Preserves queue structure and worker processes
#
# SAFETY NOTES:
# ============
# - This is a DESTRUCTIVE operation - all queued work will be lost
# - Running tasks will continue but won't be able to store results
# - Workers remain running and can accept new tasks
# - Safe to run multiple times
#
# TROUBLESHOOTING:
# ===============
# - If container name is different, adjust the docker exec command
# - If script not found, check the volume mount in docker-compose.yml
# - If permission denied, ensure script is executable: chmod +x clear_celery_queue.sh
# - If MongoDB connection fails, check MONGO_* environment variables

set -e

echo "Starting Celery queue clear operation..."

# Check if we're in the right environment
if [ -f "/usr/local/bin/clear_celery_queue.sh" ]; then
    # Running in queue container
    CONTAINER_TYPE="queue"
    echo "Running from RabbitMQ queue container"
elif [ -f "/opt/sca/app/workers/celery_app.py" ]; then
    # Running in worker container
    CONTAINER_TYPE="worker"
    echo "Running from worker container"
else
    echo "❌ Error: Script not found in expected locations. Make sure you're running this from within a container."
    echo "   Looking for:"
    echo "   - /usr/local/bin/clear_celery_queue.sh (queue container)"
    echo "   - /opt/sca/app/workers/celery_app.py (worker container)"
    exit 1
fi

# Get environment variables
QUEUE_URL=${QUEUE_URL:-"queue:5672"}
QUEUE_USER=${QUEUE_USER:-"guest"}
QUEUE_PASSWORD=${QUEUE_PASSWORD:-"guest"}
VHOST=${VHOST:-"myvhost"}

echo "Queue URL: $QUEUE_URL"
echo "Vhost: $VHOST"


# Function to use Celery control commands
purge_celery_queues() {
    echo ""
    echo "=== PURGING VIA CELERY CONTROL ==="
    
    if [ "$CONTAINER_TYPE" = "worker" ] && command -v celery >/dev/null 2>&1; then
        echo "Purging all Celery queues via Celery control..."
        
        # Change to the workers directory
        cd /opt/sca/app
        
        # Purge all queues - this is the proper way to clear Celery queues
        echo "Executing: celery -A workers.celery_app purge -f"
        if celery -A workers.celery_app purge -f; then
            echo "  ✅ All Celery queues purged successfully"
            return 0
        else
            echo "  ❌ Failed to purge Celery queues"
            return 1
        fi
    else
        echo "  ❌ ERROR: Celery command not available! This script must be run from a worker container."
        echo "     Run: docker exec -it cmg-bioloop-2-celery_worker-1 /opt/sca/app/clear_celery_queue.sh"
        return 1
    fi
}

# Main execution
echo ""
echo "=== INITIAL STATE ==="
echo "Checking queue status..."

# Try to get queue information
if command -v rabbitmqctl >/dev/null 2>&1; then
    echo "Active queues:"
    rabbitmqctl list_queues name messages -p "$VHOST" 2>/dev/null || echo "  - Could not list queues"
else
    echo "  - rabbitmqctl not available for queue inspection"
fi

# Purge Celery queues (this is the proper way - Celery manages its RabbitMQ queues)
purge_celery_queues
celery_exit_code=$?

# Final summary
echo ""
echo "=== CLEAR OPERATION SUMMARY ==="

if [ "$celery_exit_code" -eq 0 ]; then
    echo "✅ Celery queue clear operation completed successfully!"
else
    echo "❌ Celery queue clear operation failed!"
    echo "   Make sure you're running this from a worker container:"
    echo "   docker exec -it cmg-bioloop-2-celery_worker-1 /opt/sca/app/clear_celery_queue.sh"
    exit 1
fi

echo ""
echo "Note: This script clears queued messages only."
echo "Active workers and their processes are not affected."

echo ""
echo "To verify the cleanup:"
echo "  - Check RabbitMQ management UI: http://localhost:15672"
echo "  - Check Celery status: docker exec -it cmg-bioloop-2-celery_worker-1 celery -A workers.celery_app inspect active"

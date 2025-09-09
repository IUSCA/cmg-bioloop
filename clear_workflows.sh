#!/bin/bash
#
# Master Workflow Clear Script
# 
# This script orchestrates the clearing of workflows across all systems:
# 1. Clears Celery queues (stops new tasks)
# 2. Clears workflows from MongoDB (gets deleted workflow IDs)
# 3. Clears corresponding workflow records from PostgreSQL
#
# USAGE EXAMPLES:
# ===============
#
# 1. Clear workflows by status:
#    ./clear_workflows.sh --status PENDING
#    ./clear_workflows.sh -s SUCCESS
#
# 2. Clear ALL workflows:
#    ./clear_workflows.sh --status ALL
#    ./clear_workflows.sh
#
# 3. Dry run (show what would be deleted):
#    ./clear_workflows.sh --status PENDING --dry-run
#
# 4. Skip Celery queue clearing:
#    ./clear_workflows.sh --status PENDING --skip-celery
#
# PREREQUISITES:
# ==============
# - Docker containers must be running (celery_worker, mongo, postgres)
# - Scripts must be properly mounted in containers
# - User must have docker exec permissions

set -e

# Parse command line arguments
WORKFLOW_STATUS=""
DRY_RUN=false
SKIP_CELERY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--status)
            WORKFLOW_STATUS="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --skip-celery)
            SKIP_CELERY=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  -s, --status STATUS   Clear workflows with specific status (PENDING, SUCCESS, FAILURE, REVOKED)"
            echo "                        Use 'ALL' to clear all workflows regardless of status"
            echo "                        If not provided, defaults to PENDING"
            echo "  --dry-run            Show what would be deleted without actually deleting"
            echo "  --skip-celery        Skip Celery queue clearing step"
            echo "  -h, --help           Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --status PENDING   # Clear only PENDING workflows"
            echo "  $0 -s SUCCESS         # Clear only SUCCESS workflows"
            echo "  $0 --status ALL       # Clear ALL workflows"
            echo "  $0                    # Clear PENDING workflows (default)"
            echo "  $0 --dry-run          # Show what would be deleted"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Default to PENDING if no status specified
if [ -z "$WORKFLOW_STATUS" ]; then
    WORKFLOW_STATUS="PENDING"
fi

echo "🧹 Master Workflow Clear Script"
echo "==============================="
echo "Status Filter: $WORKFLOW_STATUS"
echo "Dry Run: $DRY_RUN"
echo "Skip Celery: $SKIP_CELERY"
echo ""

if [ "$DRY_RUN" = true ]; then
    echo "🔍 DRY RUN MODE - No actual deletion will occur"
    echo ""
fi

# Step 1: Clear Celery Queue (unless skipped)
if [ "$SKIP_CELERY" = false ]; then
    echo "📋 Step 1: Clearing Celery queues..."
    echo "====================================="
    
    if [ "$DRY_RUN" = true ]; then
        echo "🔍 [DRY RUN] Would clear Celery queues"
    else
        if docker exec -it cmg-bioloop-2-celery_worker-1 /opt/sca/app/clear_celery_queue.sh; then
            echo "✅ Celery queues cleared successfully"
        else
            echo "❌ Failed to clear Celery queues"
            exit 1
        fi
    fi
    echo ""
else
    echo "⏭️  Step 1: Skipping Celery queue clearing (--skip-celery)"
    echo ""
fi

# Step 2: Get workflow IDs from MongoDB and clear them
echo "🗃️  Step 2: Getting workflow IDs from MongoDB and clearing..."
echo "============================================================"

# Create a temporary file to store deleted workflow IDs
TEMP_IDS_FILE=$(mktemp)
trap "rm -f $TEMP_IDS_FILE" EXIT

if [ "$DRY_RUN" = true ]; then
    # In dry run mode, just get the IDs that would be deleted
    echo "🔍 [DRY RUN] Getting workflow IDs that would be deleted from MongoDB..."
    
    docker exec -it cmg-bioloop-2-mongo-1 mongosh --quiet --username root --password example --authenticationDatabase admin --eval "
        var currentDb = db.getSiblingDB('celery');
        var query = {};
        
        if ('$WORKFLOW_STATUS' === 'ALL') {
            query = {};
        } else {
            query = {'_status': '$WORKFLOW_STATUS'};
        }
        
        var workflows = currentDb.workflow_meta.find(query, {'_id': 1}).toArray();
        print('Found ' + workflows.length + ' workflows that would be deleted:');
        workflows.forEach(function(wf) {
            print(wf._id);
        });
    " > "$TEMP_IDS_FILE"
    
    # Extract just the workflow IDs
    grep -E '^[a-f0-9-]{36}$' "$TEMP_IDS_FILE" > "${TEMP_IDS_FILE}.clean" || true
    mv "${TEMP_IDS_FILE}.clean" "$TEMP_IDS_FILE"
    
else
    # Actually delete and capture the IDs
    echo "🗑️  Clearing workflows from MongoDB and capturing deleted IDs..."
    
    # First, get the IDs that will be deleted
    docker exec -it cmg-bioloop-2-mongo-1 mongosh --quiet --username root --password example --authenticationDatabase admin --eval "
        var currentDb = db.getSiblingDB('celery');
        var query = {};
        
        if ('$WORKFLOW_STATUS' === 'ALL') {
            query = {};
        } else {
            query = {'_status': '$WORKFLOW_STATUS'};
        }
        
        var workflows = currentDb.workflow_meta.find(query, {'_id': 1}).toArray();
        workflows.forEach(function(wf) {
            print(wf._id);
        });
    " | grep -E '^[a-f0-9-]{36}$' > "$TEMP_IDS_FILE" || true
    
    # Now delete them using our existing script
    if docker exec -it cmg-bioloop-2-mongo-1 /docker-entrypoint-initdb.d/clear_mongo.sh --status "$WORKFLOW_STATUS"; then
        echo "✅ MongoDB workflows cleared successfully"
    else
        echo "❌ Failed to clear MongoDB workflows"
        exit 1
    fi
fi

# Check if we got any workflow IDs
WORKFLOW_COUNT=$(wc -l < "$TEMP_IDS_FILE")
echo "📊 Found $WORKFLOW_COUNT workflow IDs to process"

if [ "$WORKFLOW_COUNT" -eq 0 ]; then
    echo "ℹ️  No workflows found with status: $WORKFLOW_STATUS"
    echo "✅ Nothing to clear from PostgreSQL"
    exit 0
fi

echo ""

# Step 3: Clear corresponding records from PostgreSQL
echo "🐘 Step 3: Clearing corresponding workflows from PostgreSQL..."
echo "============================================================="

if [ "$DRY_RUN" = true ]; then
    echo "🔍 [DRY RUN] Would delete these workflow IDs from PostgreSQL:"
    cat "$TEMP_IDS_FILE"
    echo ""
    echo "🔍 [DRY RUN] SQL that would be executed:"
    echo "DELETE FROM workflow WHERE id IN ("
    sed "s/^/'/" "$TEMP_IDS_FILE" | sed "s/$/'/" | paste -sd, - | sed 's/^/  /' 
    echo ");"
else
    echo "🗑️  Deleting $WORKFLOW_COUNT workflows from PostgreSQL..."
    
    # Build the SQL query
    if [ "$WORKFLOW_COUNT" -eq 1 ]; then
        # Single ID
        WORKFLOW_ID=$(cat "$TEMP_IDS_FILE")
        SQL_QUERY="DELETE FROM workflow WHERE id = '$WORKFLOW_ID';"
    else
        # Multiple IDs - build IN clause
        IDS_LIST=$(sed "s/^/'/" "$TEMP_IDS_FILE" | sed "s/$/'/" | paste -sd, -)
        SQL_QUERY="DELETE FROM workflow WHERE id IN ($IDS_LIST);"
    fi
    
    echo "🔧 Executing SQL: $SQL_QUERY"
    
    if docker exec -it cmg-bioloop-2-postgres-1 psql -U appuser -d app -c "$SQL_QUERY"; then
        echo "✅ PostgreSQL workflows cleared successfully"
    else
        echo "❌ Failed to clear PostgreSQL workflows"
        exit 1
    fi
fi

echo ""
echo "🎉 WORKFLOW CLEAR OPERATION SUMMARY"
echo "==================================="
if [ "$DRY_RUN" = true ]; then
    echo "🔍 DRY RUN completed - no actual changes made"
    echo "📊 Would have processed $WORKFLOW_COUNT workflows with status: $WORKFLOW_STATUS"
    if [ "$SKIP_CELERY" = false ]; then
        echo "📋 Would have cleared Celery queues"
    fi
    echo "🗃️  Would have cleared workflows from MongoDB"
    echo "🐘 Would have cleared $WORKFLOW_COUNT workflow records from PostgreSQL"
else
    echo "✅ Successfully cleared $WORKFLOW_COUNT workflows with status: $WORKFLOW_STATUS"
    if [ "$SKIP_CELERY" = false ]; then
        echo "📋 ✅ Celery queues cleared"
    fi
    echo "🗃️  ✅ MongoDB workflows cleared"
    echo "🐘 ✅ PostgreSQL workflow records cleared"
    
    echo ""
    echo "🔄 To verify the cleanup:"
    echo "  - Refresh the /workflows page in the UI"
    echo "  - Check MongoDB: db.workflow_meta.countDocuments({})"
    echo "  - Check PostgreSQL: SELECT COUNT(*) FROM workflow;"
fi

echo ""
echo "📝 Note: This script only clears workflow data and queues."
echo "   Active worker processes and container state are not affected."

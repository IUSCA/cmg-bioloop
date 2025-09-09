#!/bin/bash

# MongoDB Clear Script
# 
# This script clears all collections in the MongoDB database used by the Bioloop project.
# It should be run from within the MongoDB container.
# 
# USAGE EXAMPLES:
# ===============
# 
# 1. Clear workflows by status:
#    docker exec -it cmg-bioloop-2-mongo-1 /docker-entrypoint-initdb.d/clear_mongo.sh --status PENDING
#    docker exec -it cmg-bioloop-2-mongo-1 /docker-entrypoint-initdb.d/clear_mongo.sh -s SUCCESS
# 
# 2. Clear ALL workflows (any status):
#    docker exec -it cmg-bioloop-2-mongo-1 /docker-entrypoint-initdb.d/clear_mongo.sh --status ALL
#    docker exec -it cmg-bioloop-2-mongo-1 /docker-entrypoint-initdb.d/clear_mongo.sh
# 
# 3. Run interactively:
#    docker exec -it cmg-bioloop-2-mongo-1 bash
#    # /docker-entrypoint-initdb.d/clear_mongo.sh --status PENDING
# 
# 4. Run with verbose output:
#    docker exec -it cmg-bioloop-2-mongo-1 bash -c "set -x; /docker-entrypoint-initdb.d/clear_mongo.sh -s PENDING"
# 
# 4. Run with specific database (if different from default):
#    docker exec -it cmg-bioloop-2-mongo-1 bash -c "MONGO_DB=your_db_name /docker-entrypoint-initdb.d/clear_mongo.sh"
# 
# PREREQUISITES:
# ==============
# - MongoDB container must be running
# - Container name should be 'cmg-bioloop-2-mongo-1' (adjust if different)
# - Script must be mounted in the container at /docker-entrypoint-initdb.d/
# - mongosh must be available in the container
# 
# WHAT IT CLEARS:
# ===============
# - Clears 'workflow_meta' collection: removes workflows with 'Pending' status
# - Clears 'celery_taskmeta' collection: removes all Celery task results
# - Preserves database structure and indexes
# 
# SAFETY NOTES:
# ============
# - This is a DESTRUCTIVE operation - all data will be lost
# - Make backups before running if you need to preserve data
# - Only clears data, not database structure
# - Safe to run multiple times
# 
# TROUBLESHOOTING:
# ===============
# - If container name is different, adjust the docker exec command
# - If script not found, check the volume mount in docker-compose.yml
# - If permission denied, ensure script is executable: chmod +x clear_mongo.sh
# - If mongosh not found, check if MongoDB container has mongosh installed

set -e

# Parse command line arguments
WORKFLOW_STATUS=""
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--status)
            WORKFLOW_STATUS="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  -s, --status STATUS   Clear workflows with specific status (PENDING, SUCCESS, FAILURE, REVOKED)"
            echo "                        Use 'ALL' to clear all workflows regardless of status"
            echo "                        If not provided, defaults to PENDING"
            echo "  -h, --help           Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --status PENDING   # Clear only PENDING workflows"
            echo "  $0 -s SUCCESS         # Clear only SUCCESS workflows"
            echo "  $0 --status ALL       # Clear ALL workflows"
            echo "  $0                    # Clear PENDING workflows (default)"
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

# Configuration - use environment variables from .env.default
MONGO_HOST=${MONGO_HOST:-"localhost"}
MONGO_PORT=${MONGO_PORT:-"27017"}
MONGO_USER=${MONGO_INITDB_ROOT_USERNAME:-"root"}
MONGO_PASSWORD=${MONGO_INITDB_ROOT_PASSWORD:-"example"}
MONGO_AUTH_SOURCE=${MONGO_AUTH_SOURCE:-"admin"}
MONGO_DB="celery"

echo "Starting MongoDB clear operation..."
echo "Host: $MONGO_HOST:$MONGO_PORT"
echo "User: $MONGO_USER"
echo "Auth Source: $MONGO_AUTH_SOURCE"
echo "Database: $MONGO_DB"
echo "Status Filter: $WORKFLOW_STATUS"

# Check if mongosh is available
if ! command -v mongosh >/dev/null 2>&1; then
    echo "❌ Error: mongosh not found. Make sure you're running this from within the MongoDB container."
    exit 1
fi

# Function to get collection info
get_collection_info() {
    echo ""
    echo "Searching for workflow_meta and celery_taskmeta collections across all databases..."
    
    # Get list of all databases and search for our collections
    mongosh --quiet --username "$MONGO_USER" --password "$MONGO_PASSWORD" --authenticationDatabase "$MONGO_AUTH_SOURCE" --eval "
        var adminDb = db.getSiblingDB('admin');
        var databases = adminDb.runCommand('listDatabases').databases;
        
        var foundWorkflowMeta = false;
        var foundCeleryTaskMeta = false;
        
        databases.forEach(function(dbInfo) {
            var dbName = dbInfo.name;
            if (dbName === 'admin' || dbName === 'local' || dbName === 'config') return;
            
            var currentDb = db.getSiblingDB(dbName);
            var collections = currentDb.getCollectionNames();
            
            if (collections.includes('workflow_meta')) {
                foundWorkflowMeta = true;
                var pending_workflows = currentDb.workflow_meta.countDocuments({'_status': 'PENDING'});
                var totalWorkflows = currentDb.workflow_meta.countDocuments({});
                print('  Found workflow_meta in database: ' + dbName);
                print('    - Total workflows: ' + totalWorkflows);
                print('    - Pending workflows: ' + pending_workflows);
            }
            
            // Todo: only delete step data of workflows being deleted
            if (collections.includes('celery_taskmeta')) {
                foundCeleryTaskMeta = true;
                var totalTasks = currentDb.celery_taskmeta.countDocuments({});
                print('  Found celery_taskmeta in database: ' + dbName);
                print('    - Total tasks: ' + totalTasks);
            }
        });
        
        if (!foundWorkflowMeta) {
            print('  - workflow_meta collection not found in any database');
        }
        if (!foundCeleryTaskMeta) {
            print('  - celery_taskmeta collection not found in any database');
        }
    "
}

# Function to clear specific collections
clear_collections() {
    echo ""
    echo "Clearing workflow_meta and celery_taskmeta collections across all databases..."
    
    mongosh --quiet --username "$MONGO_USER" --password "$MONGO_PASSWORD" --authenticationDatabase "$MONGO_AUTH_SOURCE" --eval "
        var currentDb = db.getSiblingDB('$MONGO_DB');
        var collections = currentDb.getCollectionNames();
        var totalCleared = 0;
        
        // Clear workflow_meta: remove workflows by status
        if (collections.includes('workflow_meta')) {
            var query = {};
            var statusText = '';
            
            if ('$WORKFLOW_STATUS' === 'ALL') {
                query = {};
                statusText = 'ALL';
            } else {
                query = {'_status': '$WORKFLOW_STATUS'};
                statusText = '$WORKFLOW_STATUS';
            }
            
            var workflowCount = currentDb.workflow_meta.countDocuments(query);
            if (workflowCount > 0) {
                var result = currentDb.workflow_meta.deleteMany(query);
                print('  ✓ Cleared ' + result.deletedCount + ' ' + statusText + ' workflows from workflow_meta');
                totalCleared += result.deletedCount;
            } else {
                print('  - No ' + statusText + ' workflows found in workflow_meta');
            }
        } else {
            print('  ⚠️  workflow_meta collection not found in database $MONGO_DB');
        }
        
        // Clear celery_taskmeta: remove all task results
        if (collections.includes('celery_taskmeta')) {
            var taskCount = currentDb.celery_taskmeta.countDocuments({});
            if (taskCount > 0) {
                var result = currentDb.celery_taskmeta.deleteMany({});
                print('  ✓ Cleared ' + result.deletedCount + ' task results from celery_taskmeta');
                totalCleared += result.deletedCount;
            } else {
                print('  - celery_taskmeta collection was already empty');
            }
        } else {
            print('  ⚠️  celery_taskmeta collection not found in database $MONGO_DB');
        }
        
        print('\\nSummary:');
        print('  Total documents removed: ' + totalCleared);
    "
}

# Function to verify specific collections are cleared
verify_empty() {
    echo ""
    echo "Verifying collections are cleared across all databases:"
    
    mongosh --quiet --username "$MONGO_USER" --password "$MONGO_PASSWORD" --authenticationDatabase "$MONGO_AUTH_SOURCE" --eval "
        var adminDb = db.getSiblingDB('admin');
        var databases = adminDb.runCommand('listDatabases').databases;
        var allCleared = true;
        
        databases.forEach(function(dbInfo) {
            var dbName = dbInfo.name;
            if (dbName === 'admin' || dbName === 'local' || dbName === 'config') return;
            
            var currentDb = db.getSiblingDB(dbName);
            var collections = currentDb.getCollectionNames();
            
            // Check workflow_meta for Pending workflows
            if (collections.includes('workflow_meta')) {
                var pending_workflows = currentDb.workflow_meta.countDocuments({'_status': 'PENDING'});
                if (pending_workflows > 0) {
                    print('  ✗ workflow_meta in ' + dbName + ' still has ' + pending_workflows + ' Pending workflows');
                    allCleared = false;
                } else {
                    print('  ✓ workflow_meta in ' + dbName + ': No Pending workflows remaining');
                }
            }
            
            // Todo: only delete step data of workflows being deleted
            // Check celery_taskmeta
            if (collections.includes('celery_taskmeta')) {
                var taskCount = currentDb.celery_taskmeta.countDocuments({});
                if (taskCount > 0) {
                    print('  ✗ celery_taskmeta in ' + dbName + ' still has ' + taskCount + ' task results');
                    allCleared = false;
                } else {
                    print('  ✓ celery_taskmeta in ' + dbName + ': All task results cleared');
                }
            }
        });
        
        if (allCleared) {
            print('\\n✅ Target collections are properly cleared!');
        } else {
            print('\\n❌ Some target data still remains!');
        }
    "
}

# Main execution
echo ""
echo "=== INITIAL STATE ==="
get_collection_info

echo ""
echo "=== CLEARING COLLECTIONS ==="
clear_collections

echo ""
echo "=== VERIFICATION ==="
verify_empty

echo ""
echo "=== CLEAR OPERATION SUMMARY ==="
echo "✅ MongoDB clear operation completed!"
echo ""
echo "Note: This script only clears data. Database indexes and structure remain intact."
echo ""
echo "To verify the cleanup:"
echo "  - Check collections: mongosh --username $MONGO_USER --password $MONGO_PASSWORD --authenticationDatabase $MONGO_AUTH_SOURCE --eval \"db.getCollectionNames()\""
echo "  - Check specific collection: mongosh --username $MONGO_USER --password $MONGO_PASSWORD --authenticationDatabase $MONGO_AUTH_SOURCE --eval \"db.your_collection.countDocuments()\""

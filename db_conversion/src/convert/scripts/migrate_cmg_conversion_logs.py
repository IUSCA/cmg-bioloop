"""
CMG to CMG-Bioloop Log Migration Script

This script migrates historic CMG conversion logs to the CMG-Bioloop database.
CMG stored multiple conversions' logs in single files named convert_[dataset_name].log.
All conversions run against the same sequencing run (dataset) share the same log file.

Since the CMG-Bioloop schema cannot link the same log entries to multiple conversions,
we must duplicate log entries for each conversion that shared a log file.

IMPORTANT SAFETY NOTES:
- This script is READ-ONLY with respect to CMG:
  * Only READS CMG log files (never writes to them)
  * Does NOT connect to CMG database
  * Does NOT modify any CMG files or directories
- This script is WRITE-ONLY with respect to CMG-Bioloop:
  * WRITES to CMG-Bioloop PostgreSQL database only
  * Updates conversion.workflow_id in CMG-Bioloop database
  * Inserts into workflow, worker_process, and log tables in CMG-Bioloop database
"""

import logging
import os
import re
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Print to console
    ]
)
logger = logging.getLogger(__name__)


class CMGLogMigrationManager:
    """Manages the migration of CMG conversion logs to CMG-Bioloop database."""

    def __init__(self, pg_conn_env_vars: dict):
        """
        Initialize the migration manager.

        Args:
            pg_conn_env_vars: PostgreSQL connection environment variables for CMG-Bioloop database
                              (NOT CMG database - this script never connects to CMG)
        """
        # SAFETY: Connect ONLY to CMG-Bioloop PostgreSQL database
        # This script NEVER connects to CMG database
        self.postgres_conn = psycopg2.connect(
            host=pg_conn_env_vars['PG_HOST'],
            port=pg_conn_env_vars['PG_PORT'],
            database=pg_conn_env_vars['PG_DATABASE'],
            user=pg_conn_env_vars['PG_USER'],
            password=pg_conn_env_vars['PG_PASSWORD']
        )
        
        # Use autocommit for individual operations
        self.postgres_conn.set_session(autocommit=True)
        
        self.pg_cursor = self.postgres_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def find_cmg_log_file(self, dataset_name: str) -> Optional[str]:
        """
        Find CMG log file for a given dataset name.
        
        SAFETY: This function is READ-ONLY - only checks if file exists, never writes.
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Path to log file if found, None otherwise
        """
        path = f"/N/project/CMG-SCA/production/runlogs/convert_{dataset_name}.log"
        
        # READ-ONLY check - only verifies file existence, never modifies
        if os.path.exists(path):
            logger.info(f"Found log file at: {path}")
            return path
        
        logger.warning(f"Log file not found at: {path}")

        return None

    def ensure_workflow_id(self, conversion: dict) -> str:
        """
        Ensure conversion has a workflow_id, creating one if needed.
        
        Args:
            conversion: Conversion record from database
            
        Returns:
            workflow_id string
        """
        # Create dummy workflow_id for historic conversions using UUID
        dummy_workflow_id = str(uuid.uuid4())
        
        # SAFETY: Updates ONLY CMG-Bioloop database (conversion table), NOT CMG database
        # Update conversion record in CMG-Bioloop database
        self.pg_cursor.execute(
            """
            UPDATE conversion
            SET workflow_id = %s
            WHERE id = %s
            """,
            (str(dummy_workflow_id), int(conversion['id']))
        )
        logger.info(f"Created workflow_id {dummy_workflow_id} for conversion {conversion['cmg_id']}")
        
        return dummy_workflow_id

    def create_workflow_if_not_exists(self, workflow_id: str, dataset_id: Optional[int]) -> None:
        """
        Create workflow record if it doesn't exist.
        
        Args:
            workflow_id: Workflow ID
            dataset_id: Optional dataset ID
        """
        # SAFETY: Inserts ONLY into CMG-Bioloop database (workflow table), NOT CMG database
        self.pg_cursor.execute(
            """
            INSERT INTO workflow (id, dataset_id)
            VALUES (%s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (str(workflow_id), int(dataset_id) if dataset_id is not None else None)
        )

    def create_worker_process_for_conversion(self, conversion: dict, dataset_name: str) -> int:
        """
        Create a worker_process record for a CMG conversion.
        
        Args:
            conversion: Conversion record from database
            dataset_name: Name of the dataset
            
        Returns:
            worker_process_id
        """
        workflow_id = self.ensure_workflow_id(conversion)
        
        # Ensure workflow exists
        self.create_workflow_if_not_exists(workflow_id, conversion.get('dataset_id'))
        
        # Create worker_process record
        tags = {
            'source': 'cmg-migration',
            'cmg_id': conversion['cmg_id'],
            'dataset_name': dataset_name
        }
        
        # Ensure start_time is a datetime object, not a string
        start_time = conversion['initiated_at']
        if isinstance(start_time, str):
            # Parse string to datetime if needed
            try:
                start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                start_time = datetime.now()
        
        # SAFETY: Inserts ONLY into CMG-Bioloop database (worker_process table), NOT CMG database
        self.pg_cursor.execute(
            """
            INSERT INTO worker_process (pid, task_id, step, workflow_id, tags, start_time, hostname)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                int(0),  # Dummy PID for historic data - explicitly int
                str(f"cmg-conversion-{conversion['cmg_id']}"),  # task_id: String
                str('conversion'),  # step: String
                str(workflow_id),  # workflow_id: String
                tags,  # tags: JSONB - pass dict directly, psycopg2 handles conversion
                start_time,  # start_time: DateTime
                str('cmg-historic')  # hostname: String
            )
        )
        
        result = self.pg_cursor.fetchone()
        worker_process_id = result['id']
        logger.info(f"Created worker_process {worker_process_id} for conversion {conversion['cmg_id']}")
        return worker_process_id

    def extract_timestamp_from_line(self, log_line: str) -> Optional[datetime]:
        """
        Extract timestamp from a log line.
        
        CMG timestamp format examples:
        - "2023-10-24 22:55:39 [1a82880] INFO: message"
        - "2023-10-24 22:55:39 [1a82880] Command-line invocation: ..."
        
        Args:
            log_line: Log line text
            
        Returns:
            Parsed datetime or None if not found
        """
        pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
        match = re.search(pattern, log_line)
        
        if match:
            try:
                return datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S')
            except ValueError:
                pass
        return None

    def determine_log_level(self, log_line: str) -> str:
        """
        Determine log level from log line content.
        
        CMG log format examples:
        - "2023-10-24 22:55:39 [1a82880] INFO: message"
        - "2023-10-24 22:55:39 [1a82880] ERROR: message"
        - "2023-10-24 22:55:39 [1a82880] WARNING: message"
        
        Args:
            log_line: Log line text
            
        Returns:
            Log level (ERROR, WARNING, INFO, or DEBUG)
        """
        line_lower = log_line.lower()
        
        # First check for explicit log level prefixes (most reliable)
        # Pattern: timestamp [process_id] LEVEL: message
        if re.search(r'\]\s*(error|exception|fatal):', line_lower):
            return 'ERROR'
        elif re.search(r'\]\s*warning:', line_lower):
            return 'WARNING'
        elif re.search(r'\]\s*info:', line_lower):
            return 'INFO'
        elif re.search(r'\]\s*debug:', line_lower):
            return 'DEBUG'
        
        # Fallback to keyword matching for lines without explicit level
        if any(word in line_lower for word in ['error', 'failed', 'exception', 'fatal']):
            return 'ERROR'
        elif any(word in line_lower for word in ['warning', 'warn']):
            return 'WARNING'
        elif any(word in line_lower for word in ['info', 'completed', 'finished', 'success']):
            return 'INFO'
        else:
            return 'DEBUG'

    def migrate_log_entries(self, conversion: dict, log_file_content: str, worker_process_id: int) -> int:
        """
        Parse log file content and insert log entries for a conversion.
        
        Args:
            conversion: Conversion record
            log_file_content: Full content of the log file
            worker_process_id: ID of the worker_process record
            
        Returns:
            Number of log entries created
        """
        log_lines = log_file_content.split('\n')
        log_entries = []
        
        # Use conversion initiated_at as fallback timestamp
        fallback_timestamp = conversion['initiated_at']
        
        for line in log_lines:
            if line.strip():  # Skip empty lines
                # Extract timestamp from line
                timestamp = self.extract_timestamp_from_line(line)
                if not timestamp:
                    timestamp = fallback_timestamp
                
                # Determine log level
                level = self.determine_log_level(line)
                
                log_entries.append({
                    'timestamp': timestamp,
                    'message': line,
                    'level': level,
                    'worker_process_id': worker_process_id
                })
        
        # Batch insert for performance (1000 entries at a time)
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(log_entries), batch_size):
            batch = log_entries[i:i + batch_size]
            
            # SAFETY: Inserts ONLY into CMG-Bioloop database (log table), NOT CMG database
            # Use execute_values for efficient batch insert
            # Ensure all types are correct: timestamp (DateTime), message (String), level (String), worker_process_id (Int)
            psycopg2.extras.execute_values(
                self.pg_cursor,
                """
                INSERT INTO log (timestamp, message, level, worker_process_id)
                VALUES %s
                """,
                [
                    (
                        entry['timestamp'],  # DateTime
                        str(entry['message']),  # String
                        str(entry['level']),  # String
                        int(entry['worker_process_id'])  # Int
                    )
                    for entry in batch
                ],
                template='(%s, %s, %s, %s)',
                page_size=batch_size
            )
            
            total_inserted += len(batch)
        
        logger.info(f"Inserted {total_inserted} log entries for worker_process {worker_process_id}")
        return total_inserted

    def group_conversions_by_dataset(self) -> Dict[int, List[dict]]:
        """
        Query and group CMG conversions by dataset.
        
        SAFETY: Reads ONLY from CMG-Bioloop database (conversion and dataset tables), NOT CMG database.
        
        Returns:
            Dictionary mapping dataset_id to list of conversions
        """
        # SAFETY: SELECT query - read-only operation on CMG-Bioloop database
        self.pg_cursor.execute(
            """
            SELECT 
                c.id,
                c.cmg_id,
                c.dataset_id,
                d.name as dataset_name,
                c.workflow_id,
                c.initiated_at
            FROM conversion c
            JOIN dataset d ON d.id = c.dataset_id
            WHERE c.cmg_id IS NOT NULL
            ORDER BY c.dataset_id, c.initiated_at
            """
        )
        
        conversions = self.pg_cursor.fetchall()
        
        # Group by dataset_id
        conversions_by_dataset = defaultdict(list)
        for conv in conversions:
            conversions_by_dataset[conv['dataset_id']].append(conv)
        
        return conversions_by_dataset

    def migrate_conversion(self, conversion: dict, log_content: str, dataset_name: str) -> Tuple[bool, int]:
        """
        Migrate a single conversion.
        
        Args:
            conversion: Conversion record
            log_content: Log file content
            dataset_name: Name of the dataset
            
        Returns:
            Tuple of (success: bool, log_count: int)
        """
        try:
            worker_process_id = self.create_worker_process_for_conversion(conversion, dataset_name)
            log_count = self.migrate_log_entries(conversion, log_content, worker_process_id)
            return True, log_count
        except Exception as e:
            logger.error(f"Error migrating conversion {conversion['cmg_id']}: {e}", exc_info=True)
            return False, 0

    def migrate_cmg_conversion_logs(self):
        """
        Main migration function that orchestrates the entire process.
        """
        logger.info("🚀 Starting CMG to CMG-Bioloop log migration...")
        
        # 1. Group conversions by dataset (since they share log files)
        conversions_by_dataset = self.group_conversions_by_dataset()
        
        logger.info(f"Found {len(conversions_by_dataset)} unique datasets with CMG conversions")
        
        migration_stats = {
            'datasets_processed': 0,
            'conversions_migrated': 0,
            'log_entries_created': 0,
            'missing_log_files': [],
            'failed_conversions': []
        }
        
        for dataset_id, conversions in conversions_by_dataset.items():
            dataset_name = conversions[0]['dataset_name']
            logger.info(f"\n📁 Processing dataset: {dataset_name}")
            logger.info(f"🔄 Found {len(conversions)} conversions to migrate")
            
            # 2. Find the shared log file
            log_file_path = self.find_cmg_log_file(dataset_name)
            if not log_file_path:
                logger.warning(f"⚠️  Log file not found for dataset: {dataset_name}")
                migration_stats['missing_log_files'].append(dataset_name)
                continue
            
            # 3. Read log file content once (READ-ONLY - never writes to CMG log files)
            try:
                # SAFETY: Opens file in read-only mode ('r') - never writes to CMG files
                with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    log_content = f.read()
            except Exception as e:
                logger.error(f"Error reading log file {log_file_path}: {e}")
                migration_stats['missing_log_files'].append(dataset_name)
                continue
            
            logger.info(f"📄 Log file size: {len(log_content):,} characters")
            
            # 4. For each conversion, create worker_process and duplicate log entries
            for conversion in conversions:
                logger.info(f"   🔄 Migrating conversion {conversion['cmg_id']}...")
                
                success, log_count = self.migrate_conversion(conversion, log_content, dataset_name)
                
                if success:
                    logger.info(f"   ✅ Created {log_count:,} log entries")
                    migration_stats['conversions_migrated'] += 1
                    migration_stats['log_entries_created'] += log_count
                else:
                    logger.error(f"   ❌ Failed to migrate conversion {conversion['cmg_id']}")
                    migration_stats['failed_conversions'].append(conversion['cmg_id'])
            
            migration_stats['datasets_processed'] += 1
        
        # 5. Print final statistics
        logger.info(f"\n🎉 Migration completed!")
        logger.info(f"📊 Statistics:")
        logger.info(f"   - Datasets processed: {migration_stats['datasets_processed']}")
        logger.info(f"   - Conversions migrated: {migration_stats['conversions_migrated']}")
        logger.info(f"   - Log entries created: {migration_stats['log_entries_created']:,}")
        logger.info(f"   - Missing log files: {len(migration_stats['missing_log_files'])}")
        logger.info(f"   - Failed conversions: {len(migration_stats['failed_conversions'])}")
        
        if migration_stats['missing_log_files']:
            logger.warning(f"⚠️  Datasets with missing log files:")
            for dataset in migration_stats['missing_log_files']:
                logger.warning(f"     - {dataset}")
        
        if migration_stats['failed_conversions']:
            logger.error(f"❌ Failed conversions:")
            for cmg_id in migration_stats['failed_conversions']:
                logger.error(f"     - {cmg_id}")
        
        return migration_stats

    def close_connections(self):
        """Close database connections."""
        self.postgres_conn.close()


def main():
    """
    Main function to run the CMG log migration.
    """
    load_dotenv()
    
    logger.info("Starting CMG to CMG-Bioloop log migration")
    
    # Get PostgreSQL connection variables
    postgres_db = os.getenv('PG_DATABASE')
    pg_user = os.getenv('PG_USER')
    pg_password = os.getenv('PG_PASSWORD')
    pg_host = os.getenv('PG_HOST')
    pg_port = os.getenv('PG_PORT')
    
    pg_env_vars = {
        'PG_DATABASE': postgres_db,
        'PG_USER': pg_user,
        'PG_PASSWORD': pg_password,
        'PG_HOST': pg_host,
        'PG_PORT': pg_port
    }
    
    missing_pg_env_vars = [var for var, value in pg_env_vars.items() if not value]
    
    if missing_pg_env_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_pg_env_vars)}")
    
    manager = None
    try:
        # Initialize the migration manager
        manager = CMGLogMigrationManager(pg_env_vars)
        
        # Run the migration
        stats = manager.migrate_cmg_conversion_logs()
        
        return stats
    except Exception as e:
        logger.error(f"Error during migration: {e}", exc_info=True)
        raise
    finally:
        if manager:
            manager.close_connections()


if __name__ == "__main__":
    import fire
    fire.Fire(main)


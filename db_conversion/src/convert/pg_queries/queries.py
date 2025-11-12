drop_all_enums = """
            -- Drop enum types
                DROP TYPE IF EXISTS ACCESS_TYPE CASCADE;
                DROP TYPE IF EXISTS NOTIFICATION_STATUS CASCADE;
                DROP TYPE IF EXISTS UPLOAD_STATUS CASCADE;
                DROP TYPE IF EXISTS UPLOAD_TYPE CASCADE;
                DROP TYPE IF EXISTS DATASET_CREATE_METHOD CASCADE;
                DROP TYPE IF EXISTS ARGUMENT_VALUE_TYPE CASCADE;
"""

drop_all_tables = """
                DROP TABLE IF EXISTS
                session_track, genome_browser_session, track, conversion_derived_dataset, 
                argument_value, conversion, argument, conversion_definition, cmd_line_program,
                dynamic_variable, log, worker_process, dataset_genomic_attributes, about, nonce, instrument,
                dataset_file_hierarchy, file_upload_log, dataset_upload_log, dataset_audit,
                dataset_state, bundle, data_access_log, stage_request_log,
                user_password, user_login, user_settings, notification, role_notification,
                user_notification, contact, user_role, dataset_file, dataset_hierarchy,
                project_dataset, project_user, project_contact, project, workflow, metric,
                dataset, "user", role
                CASCADE;
"""

create_all_enums = """
            -- Create enum types
                CREATE TYPE ACCESS_TYPE AS ENUM ('BROWSER', 'SLATE_SCRATCH');
                CREATE TYPE NOTIFICATION_STATUS AS ENUM ('CREATED', 'ACKNOWLEDGED', 'RESOLVED');
                CREATE TYPE UPLOAD_STATUS AS ENUM ('UPLOADING', 'UPLOAD_FAILED', 'UPLOADED', 'PROCESSING', 'PROCESSING_FAILED', 'COMPLETE', 'FAILED');
                CREATE TYPE UPLOAD_TYPE AS ENUM ('DATASET');
                CREATE TYPE DATASET_CREATE_METHOD AS ENUM ('UPLOAD', 'IMPORT', 'SCAN');
                CREATE TYPE ARGUMENT_VALUE_TYPE AS ENUM ('STRING', 'NUMBER', 'BOOLEAN');
"""

create_all_tables = """                
                CREATE TABLE "user" (
                  "id" SERIAL PRIMARY KEY,
                  "username" VARCHAR(100) UNIQUE NOT NULL,
                  "name" VARCHAR(100),
                  "email" VARCHAR(100) UNIQUE NOT NULL,
                  "cas_id" VARCHAR(100) UNIQUE,
                  "notes" TEXT,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "is_deleted" BOOLEAN NOT NULL DEFAULT false,
                  "metadata" JSONB,
                  "cmg_id" VARCHAR(100)
                );
                
                CREATE TABLE "role" (
                  "id" SERIAL PRIMARY KEY,
                  "name" VARCHAR(50) NOT NULL,
                  "description" VARCHAR(255) NOT NULL DEFAULT ''
                );
                
                -- Create tables
                CREATE TABLE "dataset" (
                  "id" SERIAL PRIMARY KEY,
                  "name" TEXT NOT NULL,
                  "type" TEXT NOT NULL,
                  "num_directories" INTEGER,
                  "num_files" INTEGER,
                  "du_size" BIGINT,
                  "size" BIGINT,
                  "bundle_size" BIGINT,
                  "description" TEXT,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "origin_path" TEXT,
                  "archive_path" TEXT,
                  "staged_path" TEXT,
                  "is_deleted" BOOLEAN NOT NULL DEFAULT false,
                  "is_staged" BOOLEAN NOT NULL DEFAULT false,
                  "metadata" JSONB,
                  "src_instrument_id" INTEGER,
                  "file_type" TEXT,
                  "cmg_id" TEXT UNIQUE,
                  UNIQUE ("name", "type", "is_deleted")
                );
                
                CREATE TABLE "dataset_hierarchy" (
                  "source_id" INTEGER NOT NULL,
                  "derived_id" INTEGER NOT NULL,
                  "assigned_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  PRIMARY KEY ("source_id", "derived_id"),
                  FOREIGN KEY ("source_id") REFERENCES "dataset"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("derived_id") REFERENCES "dataset"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "dataset_file" (
                  "id" SERIAL PRIMARY KEY,
                  "name" TEXT,
                  "path" TEXT NOT NULL,
                  "md5" TEXT,
                  "size" BIGINT,
                  "filetype" TEXT,
                  "metadata" JSONB,
                  "status" TEXT,
                  "dataset_id" INTEGER NOT NULL,
                  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE,
                  UNIQUE ("path", "dataset_id")
                );
                
                CREATE INDEX ON "dataset_file" ("dataset_id");
                
                CREATE TABLE "dataset_file_hierarchy" (
                  "parent_id" INTEGER NOT NULL,
                  "child_id" INTEGER NOT NULL,
                  PRIMARY KEY ("parent_id", "child_id"),
                  FOREIGN KEY ("parent_id") REFERENCES "dataset_file"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("child_id") REFERENCES "dataset_file"("id") ON DELETE CASCADE
                );
                
                CREATE INDEX ON "dataset_file_hierarchy" ("child_id");
                
                CREATE TABLE "dataset_audit" (
                  "id" SERIAL PRIMARY KEY,
                  "action" TEXT NOT NULL,
                  "create_method" DATASET_CREATE_METHOD,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "timestamp" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "old_data" JSONB,
                  "new_data" JSONB,
                  "user_id" INTEGER,
                  "dataset_id" INTEGER,
                  FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE SET NULL,
                  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE,
                  UNIQUE ("dataset_id", "create_method")
                );
                
                CREATE TABLE "dataset_upload_log" (
                  "id" SERIAL PRIMARY KEY,
                  "status" UPLOAD_STATUS NOT NULL,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "audit_log_id" INTEGER UNIQUE NOT NULL,
                  FOREIGN KEY ("audit_log_id") REFERENCES "dataset_audit"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "file_upload_log" (
                  "id" SERIAL PRIMARY KEY,
                  "name" TEXT NOT NULL,
                  "md5" TEXT NOT NULL,
                  "num_chunks" INTEGER NOT NULL,
                  "status" UPLOAD_STATUS NOT NULL,
                  "path" TEXT,
                  "dataset_upload_log_id" INTEGER,
                  FOREIGN KEY ("dataset_upload_log_id") REFERENCES "dataset_upload_log"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "dataset_state" (
                  "state" TEXT NOT NULL,
                  "timestamp" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "metadata" JSONB,
                  "dataset_id" INTEGER NOT NULL,
                  PRIMARY KEY ("timestamp", "dataset_id", "state"),
                  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "bundle" (
                  "id" SERIAL PRIMARY KEY,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "name" TEXT NOT NULL,
                  "size" BIGINT,
                  "md5" TEXT NOT NULL,
                  "dataset_id" INTEGER UNIQUE NOT NULL,
                  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "data_access_log" (
                  "id" SERIAL PRIMARY KEY,
                  "timestamp" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "access_type" ACCESS_TYPE NOT NULL,
                  "file_id" INTEGER,
                  "dataset_id" INTEGER,
                  "user_id" INTEGER NOT NULL,
                  FOREIGN KEY ("file_id") REFERENCES "dataset_file"("id"),
                  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id"),
                  FOREIGN KEY ("user_id") REFERENCES "user"("id")
                );
                
                CREATE TABLE "stage_request_log" (
                  "id" SERIAL PRIMARY KEY,
                  "timestamp" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "dataset_id" INTEGER,
                  "user_id" INTEGER NOT NULL,
                  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id"),
                  FOREIGN KEY ("user_id") REFERENCES "user"("id")
                );
                
                CREATE TABLE "user_password" (
                  "id" SERIAL PRIMARY KEY,
                  "password" VARCHAR(100) NOT NULL,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "user_id" INTEGER UNIQUE NOT NULL,
                  FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "user_login" (
                  "id" SERIAL PRIMARY KEY,
                  "last_login" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "method" TEXT NOT NULL,
                  "user_id" INTEGER UNIQUE NOT NULL,
                  FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "user_settings" (
                  "id" SERIAL PRIMARY KEY,
                  "user_id" INTEGER UNIQUE NOT NULL,
                  "settings" JSONB NOT NULL,
                  FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "notification" (
                  "id" SERIAL PRIMARY KEY,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "label" TEXT NOT NULL,
                  "text" TEXT,
                  "status" NOTIFICATION_STATUS NOT NULL DEFAULT 'CREATED',
                  "acknowledged_by_id" INTEGER,
                  FOREIGN KEY ("acknowledged_by_id") REFERENCES "user"("id")
                );
                
                CREATE TABLE "role_notification" (
                  "id" SERIAL PRIMARY KEY,
                  "role_id" INTEGER NOT NULL,
                  "notification_id" INTEGER NOT NULL,
                  UNIQUE ("notification_id", "role_id"),
                  FOREIGN KEY ("role_id") REFERENCES "role"("id"),
                  FOREIGN KEY ("notification_id") REFERENCES "notification"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "user_notification" (
                  "id" SERIAL PRIMARY KEY,
                  "user_id" INTEGER NOT NULL,
                  "notification_id" INTEGER NOT NULL,
                  UNIQUE ("notification_id", "user_id"),
                  FOREIGN KEY ("user_id") REFERENCES "user"("id"),
                  FOREIGN KEY ("notification_id") REFERENCES "notification"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "contact" (
                  "id" SERIAL PRIMARY KEY,
                  "type" TEXT NOT NULL,
                  "value" TEXT NOT NULL,
                  "description" TEXT,
                  "user_id" INTEGER,
                  UNIQUE ("type", "value"),
                  FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "user_role" (
                  "user_id" INTEGER NOT NULL,
                  "role_id" INTEGER NOT NULL,
                  "assigned_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  PRIMARY KEY ("user_id", "role_id"),
                  FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("role_id") REFERENCES "role"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "project" (
                  "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                  "slug" TEXT UNIQUE NOT NULL,
                  "name" TEXT NOT NULL,
                  "description" TEXT,
                  "browser_enabled" BOOLEAN NOT NULL DEFAULT false,
                  "funding" TEXT,
                  "metadata" JSONB,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "cmg_id" TEXT
                );
                
                CREATE TABLE "project_user" (
                  "project_id" UUID NOT NULL,
                  "user_id" INTEGER NOT NULL,
                  "assigned_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "assignor_id" INTEGER,
                  PRIMARY KEY ("project_id", "user_id"),
                  FOREIGN KEY ("project_id") REFERENCES "project"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("assignor_id") REFERENCES "user"("id") ON DELETE SET NULL
                );
                
                CREATE TABLE "project_dataset" (
                  "project_id" UUID NOT NULL,
                  "dataset_id" INTEGER NOT NULL,
                  "assignor_id" INTEGER,
                  "assigned_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  PRIMARY KEY ("project_id", "dataset_id"),
                  FOREIGN KEY ("project_id") REFERENCES "project"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("assignor_id") REFERENCES "user"("id") ON DELETE SET NULL
                );
                
                CREATE TABLE "project_contact" (
                  "project_id" UUID NOT NULL,
                  "contact_id" INTEGER NOT NULL,
                  "assigned_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "assignor_id" INTEGER,
                  PRIMARY KEY ("project_id", "contact_id"),
                  FOREIGN KEY ("project_id") REFERENCES "project"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("contact_id") REFERENCES "contact"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("assignor_id") REFERENCES "user"("id") ON DELETE SET NULL
                );
                
                CREATE TABLE "workflow" (
                  "id" TEXT PRIMARY KEY,
                  "dataset_id" INTEGER,
                  "initiator_id" INTEGER,
                  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("initiator_id") REFERENCES "user"("id") ON DELETE SET NULL
                );
                
                CREATE TABLE "metric" (
                  "timestamp" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "measurement" TEXT NOT NULL,
                  "subject" TEXT NOT NULL,
                  "usage" BIGINT,
                  "limit" BIGINT,
                  "fields" JSONB,
                  "tags" JSONB,
                  PRIMARY KEY ("timestamp", "measurement", "subject")
                );
                
                CREATE TABLE "dataset_genomic_attributes" (
                  "dataset_id" INTEGER NOT NULL PRIMARY KEY,
                  "genome_type" TEXT,
                  "genome_value" TEXT,
                  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "worker_process" (
                  "id" SERIAL PRIMARY KEY,
                  "pid" INTEGER NOT NULL,
                  "task_id" TEXT NOT NULL,
                  "step" TEXT NOT NULL,
                  "workflow_id" TEXT NOT NULL,
                  "tags" JSONB,
                  "start_time" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "hostname" TEXT NOT NULL
                );
                
                CREATE TABLE "log" (
                  "id" SERIAL PRIMARY KEY,
                  "timestamp" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "message" TEXT NOT NULL,
                  "level" TEXT NOT NULL,
                  "worker_process_id" INTEGER NOT NULL,
                  FOREIGN KEY ("worker_process_id") REFERENCES "worker_process"("id") ON DELETE CASCADE
                );
                
                CREATE INDEX ON "log" ("worker_process_id");
                
                CREATE TABLE "about" (
                  "id" SERIAL PRIMARY KEY,
                  "html" TEXT NOT NULL,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "last_updated_by_id" INTEGER,
                  FOREIGN KEY ("last_updated_by_id") REFERENCES "user"("id") ON DELETE SET NULL
                );
                
                CREATE TABLE "nonce" (
                  "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "purpose" TEXT,
                  "expires_at" TIMESTAMP(6)
                );
                
                CREATE TABLE "instrument" (
                  "id" SERIAL PRIMARY KEY,
                  "name" TEXT UNIQUE NOT NULL,
                  "host" TEXT UNIQUE NOT NULL,
                  "description" TEXT,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE "track" (
                  "id" SERIAL PRIMARY KEY,
                  "name" TEXT NOT NULL,
                  "dataset_file_id" INTEGER UNIQUE NOT NULL,
                  "color" TEXT,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY ("dataset_file_id") REFERENCES "dataset_file"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "genome_browser_session" (
                  "id" SERIAL PRIMARY KEY,
                  "title" TEXT,
                  "genome" TEXT,
                  "genome_type" TEXT,
                  "user_id" INTEGER,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "access_count" INTEGER NOT NULL DEFAULT 0,
                  "is_public" BOOLEAN NOT NULL DEFAULT false,
                  "staging_requested" JSONB,
                  "staging_completed" BOOLEAN NOT NULL DEFAULT false,
                  "staging_requested_by" INTEGER,
                  FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "session_track" (
                  "id" SERIAL PRIMARY KEY,
                  "session_id" INTEGER NOT NULL,
                  "track_id" INTEGER NOT NULL,
                  "color" TEXT,
                  "title" TEXT,
                  "order" INTEGER NOT NULL DEFAULT 0,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY ("session_id") REFERENCES "genome_browser_session"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("track_id") REFERENCES "track"("id") ON DELETE CASCADE,
                  UNIQUE ("session_id", "track_id")
                );
                
                CREATE TABLE "cmd_line_program" (
                  "id" SERIAL PRIMARY KEY,
                  "name" TEXT UNIQUE NOT NULL,
                  "executable_path" TEXT NOT NULL,
                  "executable_directory" TEXT,
                  "allow_additional_args" BOOLEAN NOT NULL DEFAULT false,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  UNIQUE ("name", "executable_path")
                );
                
                CREATE TABLE "conversion_definition" (
                  "id" SERIAL PRIMARY KEY,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "name" TEXT UNIQUE NOT NULL,
                  "description" TEXT,
                  "enabled" BOOLEAN NOT NULL DEFAULT false,
                  "author_id" INTEGER NOT NULL,
                  "dataset_types" TEXT[],
                  "tags" TEXT[],
                  "program_id" INTEGER NOT NULL,
                  "output_directory" TEXT,
                  "logs_directory" TEXT,
                  "capture_logs" BOOLEAN NOT NULL DEFAULT false,
                  FOREIGN KEY ("program_id") REFERENCES "cmd_line_program"("id") ON DELETE RESTRICT,
                  FOREIGN KEY ("author_id") REFERENCES "user"("id") ON DELETE RESTRICT
                );
                
                CREATE TABLE "dynamic_variable" (
                  "name" TEXT PRIMARY KEY,
                  "description" TEXT,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE "argument" (
                  "id" SERIAL PRIMARY KEY,
                  "name" TEXT,
                  "value_type" ARGUMENT_VALUE_TYPE NOT NULL,
                  "allowed_values" TEXT[],
                  "is_required" BOOLEAN NOT NULL DEFAULT false,
                  "default_value" TEXT,
                  "is_flag" BOOLEAN NOT NULL DEFAULT false,
                  "description" TEXT,
                  "min_value" DOUBLE PRECISION,
                  "max_value" DOUBLE PRECISION,
                  "min_length" INTEGER,
                  "max_length" INTEGER,
                  "position" INTEGER,
                  "dynamic_variable_name" TEXT,
                  "program_id" INTEGER NOT NULL,
                  FOREIGN KEY ("dynamic_variable_name") REFERENCES "dynamic_variable"("name") ON DELETE SET NULL,
                  FOREIGN KEY ("program_id") REFERENCES "cmd_line_program"("id") ON DELETE RESTRICT
                );
                
                CREATE TABLE "conversion" (
                  "id" SERIAL PRIMARY KEY,
                  "initiated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "definition_id" INTEGER NOT NULL,
                  "workflow_id" TEXT,
                  "dataset_id" INTEGER,
                  "initiator_id" INTEGER,
                  "additional_args" JSONB,
                  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("definition_id") REFERENCES "conversion_definition"("id") ON DELETE RESTRICT,
                  FOREIGN KEY ("initiator_id") REFERENCES "user"("id") ON DELETE SET NULL,
                  "cmg_id" TEXT
                );
                
                CREATE TABLE "argument_value" (
                  "id" SERIAL PRIMARY KEY,
                  "argument_id" INTEGER NOT NULL,
                  "conversion_id" INTEGER NOT NULL,
                  "value" TEXT,
                  FOREIGN KEY ("argument_id") REFERENCES "argument"("id") ON DELETE RESTRICT,
                  FOREIGN KEY ("conversion_id") REFERENCES "conversion"("id") ON DELETE RESTRICT
                );
                
                CREATE TABLE "conversion_derived_dataset" (
                  "conversion_id" INTEGER NOT NULL,
                  "dataset_id" INTEGER NOT NULL,
                  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "metadata" JSONB,
                  PRIMARY KEY ("conversion_id", "dataset_id"),
                  FOREIGN KEY ("conversion_id") REFERENCES "conversion"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE
                );
                
                -- Add foreign key constraints that reference tables defined later
                ALTER TABLE "dataset" ADD CONSTRAINT "dataset_src_instrument_id_fkey" 
                  FOREIGN KEY ("src_instrument_id") REFERENCES "instrument"("id");
"""

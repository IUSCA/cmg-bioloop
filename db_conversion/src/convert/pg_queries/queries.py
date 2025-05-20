drop_all_enums = """
            -- Drop enum types
                DROP TYPE IF EXISTS ACCESS_TYPE CASCADE;
                DROP TYPE IF EXISTS NOTIFICATION_STATUS CASCADE;
                DROP TYPE IF EXISTS UPLOAD_STATUS CASCADE;
"""

drop_all_tables = """
                DROP TABLE IF EXISTS
                dataset_file_hierarchy, upload_log, dataset_upload_log, file_upload_log,
                dataset_audit, dataset_state, bundle, data_access_log, stage_request_log,
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
                  "is_deleted" BOOLEAN NOT NULL DEFAULT false
                );
                
                CREATE TABLE "role" (
                  "id" SERIAL PRIMARY KEY,
                  "name" VARCHAR(50) NOT NULL,
                  "description" VARCHAR(255) NOT NULL DEFAULT ''
                );
                
                -- Create tables
                CREATE TABLE "dataset" (
                  "id" SERIAL PRIMARY KEY,
                  "cmg_id" TEXT UNIQUE NOT NULL,
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
                
                CREATE TABLE "upload_log" (
                  "id" SERIAL PRIMARY KEY,
                  "status" upload_status NOT NULL,
                  "initiated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "user_id" INTEGER NOT NULL,
                  FOREIGN KEY ("user_id") REFERENCES "user"("id")
                );
                
                CREATE TABLE "dataset_upload_log" (
                  "id" SERIAL PRIMARY KEY,
                  "dataset_id" INTEGER UNIQUE NOT NULL,
                  "upload_log_id" INTEGER UNIQUE NOT NULL,
                  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("upload_log_id") REFERENCES "upload_log"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "file_upload_log" (
                  "id" SERIAL PRIMARY KEY,
                  "name" TEXT NOT NULL,
                  "md5" TEXT NOT NULL,
                  "num_chunks" INTEGER NOT NULL,
                  "status" upload_status NOT NULL,
                  "path" TEXT,
                  "upload_log_id" INTEGER,
                  FOREIGN KEY ("upload_log_id") REFERENCES "upload_log"("id") ON DELETE CASCADE
                );
                
                CREATE TABLE "dataset_audit" (
                  "id" SERIAL PRIMARY KEY,
                  "action" TEXT NOT NULL,
                  "timestamp" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  "old_data" JSONB,
                  "new_data" JSONB,
                  "user_id" INTEGER,
                  "dataset_id" INTEGER,
                  FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE CASCADE,
                  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE
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
                  "access_type" access_type NOT NULL,
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
                  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP
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
                
"""

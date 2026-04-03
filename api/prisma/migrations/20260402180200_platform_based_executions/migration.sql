-- Platform-based executions

CREATE TYPE "execution_platform" AS ENUM ('LOCAL', 'SLURM', 'KUBERNETES', 'AWS_BATCH', 'CUSTOM');
CREATE TYPE "artifact_type" AS ENUM ('JOB_SCRIPT', 'ENVIRONMENT_SETUP', 'RUNTIME_CONFIG', 'RESOURCE_MANIFEST', 'SECRETS', 'DEPENDENCY_FILE', 'CUSTOM');
CREATE TYPE "storage_type" AS ENUM ('INLINE', 'FILE_PATH', 'EXTERNAL_URL');

ALTER TABLE "conversion"
ADD COLUMN IF NOT EXISTS "metadata" JSONB;

ALTER TABLE "worker_process"
ADD COLUMN IF NOT EXISTS "executor_job_id" TEXT;

CREATE TABLE "process_request" (
    "id" SERIAL NOT NULL,
    "execution_platform" "execution_platform" NOT NULL DEFAULT 'LOCAL',
    "job_step" TEXT,
    "conversion_id" INTEGER,
    "execution_config" JSONB,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL,
    CONSTRAINT "process_request_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "process_artifact" (
    "id" SERIAL NOT NULL,
    "process_id" INTEGER NOT NULL,
    "artifact_type" "artifact_type" NOT NULL,
    "storage_type" "storage_type" NOT NULL DEFAULT 'INLINE',
    "content_inline" TEXT,
    "file_path" TEXT,
    "external_url" TEXT,
    "supports_template" BOOLEAN NOT NULL DEFAULT false,
    "template_variables" JSONB,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL,
    CONSTRAINT "process_artifact_pkey" PRIMARY KEY ("id")
);

ALTER TABLE "process_request"
ADD CONSTRAINT "process_request_conversion_id_fkey"
FOREIGN KEY ("conversion_id") REFERENCES "conversion"("id")
ON DELETE SET NULL ON UPDATE CASCADE;

ALTER TABLE "process_artifact"
ADD CONSTRAINT "process_artifact_process_id_fkey"
FOREIGN KEY ("process_id") REFERENCES "process_request"("id")
ON DELETE CASCADE ON UPDATE CASCADE;

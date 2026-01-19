-- CreateTable
CREATE TABLE "dataset_import_log" (
    "id" SERIAL NOT NULL,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "cmg_id" VARCHAR(100),
    "file_type" TEXT,
    "genome_type" TEXT,
    "genome_value" TEXT,
    "source_run" TEXT,
    "notes" TEXT,
    "metadata" JSONB,
    "audit_log_id" INTEGER NOT NULL,

    CONSTRAINT "dataset_import_log_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "cmg_sync_cursor" (
    "poller_name" TEXT NOT NULL,
    "last_updated_at" TIMESTAMP(3),
    "last_cmg_objectid" TEXT,
    "locked_by" TEXT,
    "lock_expires_at" TIMESTAMP(3),
    "last_started_at" TIMESTAMP(3),
    "last_succeeded_at" TIMESTAMP(3),
    "last_failed_at" TIMESTAMP(3),
    "last_error" TEXT,
    "last_run_count" INTEGER,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "cmg_sync_cursor_pkey" PRIMARY KEY ("poller_name")
);

-- CreateTable
CREATE TABLE "cmg_sync_retry" (
    "id" SERIAL NOT NULL,
    "poller_name" TEXT NOT NULL,
    "cmg_id" TEXT NOT NULL,
    "failure_count" INTEGER NOT NULL DEFAULT 0,
    "last_error" TEXT,
    "next_retry_at" TIMESTAMP(3),
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "cmg_sync_retry_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "dataset_import_log_audit_log_id_key" ON "dataset_import_log"("audit_log_id");

-- CreateIndex
CREATE UNIQUE INDEX "cmg_sync_retry_poller_name_cmg_id_key" ON "cmg_sync_retry"("poller_name", "cmg_id");

-- AddForeignKey
ALTER TABLE "dataset_import_log" ADD CONSTRAINT "dataset_import_log_audit_log_id_fkey" FOREIGN KEY ("audit_log_id") REFERENCES "dataset_audit"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- Add xenium_id to existing tables

-- AlterTable
ALTER TABLE "dataset" ADD COLUMN "xenium_id" INTEGER;

-- AlterTable
ALTER TABLE "dataset_import_log" ADD COLUMN "xenium_id" INTEGER;

-- AlterTable
ALTER TABLE "user" ADD COLUMN "xenium_id" INTEGER;

-- AlterTable
ALTER TABLE "project" ADD COLUMN "xenium_id" INTEGER;

-- AlterTable
ALTER TABLE "genome_browser_session" ADD COLUMN "xenium_id" INTEGER;

-- AlterTable
ALTER TABLE "conversion" ADD COLUMN "xenium_id" INTEGER;

-- CreateTable
CREATE TABLE "xenium_sync_cursor" (
    "poller_name" TEXT NOT NULL,
    "last_updated_at" TIMESTAMP(3),
    "last_xenium_id" INTEGER,
    "locked_by" TEXT,
    "lock_expires_at" TIMESTAMP(3),
    "last_started_at" TIMESTAMP(3),
    "last_succeeded_at" TIMESTAMP(3),
    "last_failed_at" TIMESTAMP(3),
    "last_error" TEXT,
    "last_run_count" INTEGER,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "xenium_sync_cursor_pkey" PRIMARY KEY ("poller_name")
);

-- CreateTable
CREATE TABLE "xenium_sync_retry" (
    "id" SERIAL NOT NULL,
    "poller_name" TEXT NOT NULL,
    "xenium_id" INTEGER NOT NULL,
    "failure_count" INTEGER NOT NULL DEFAULT 0,
    "last_error" TEXT,
    "next_retry_at" TIMESTAMP(3),
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "xenium_sync_retry_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "xenium_sync_process_lock" (
    "process_name" VARCHAR(50) NOT NULL,
    "locked_by" VARCHAR(200),
    "lock_expires_at" TIMESTAMP(3),
    "last_started_at" TIMESTAMP(3),
    "last_completed_at" TIMESTAMP(3),
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "xenium_sync_process_lock_pkey" PRIMARY KEY ("process_name")
);

-- CreateIndex
CREATE UNIQUE INDEX "xenium_sync_retry_poller_name_xenium_id_key" ON "xenium_sync_retry"("poller_name", "xenium_id");

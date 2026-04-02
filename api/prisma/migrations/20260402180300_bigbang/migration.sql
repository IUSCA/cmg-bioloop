-- bigbang

-- CMG and Xenium source IDs tracked on migrated entities
ALTER TABLE "dataset" ADD COLUMN "cmg_id" TEXT;
ALTER TABLE "dataset" ADD COLUMN "xenium_id" INTEGER;

ALTER TABLE "dataset_import_log" ADD COLUMN "cmg_id" VARCHAR(100);
ALTER TABLE "dataset_import_log" ADD COLUMN "xenium_id" INTEGER;

ALTER TABLE "user" ADD COLUMN "cmg_id" VARCHAR(100);
ALTER TABLE "user" ADD COLUMN "xenium_id" INTEGER;

ALTER TABLE "project" ADD COLUMN "cmg_id" TEXT;
ALTER TABLE "project" ADD COLUMN "xenium_id" TEXT;

ALTER TABLE "genome_browser_session" ADD COLUMN "cmg_id" VARCHAR(100);
ALTER TABLE "genome_browser_session" ADD COLUMN "xenium_id" INTEGER;

ALTER TABLE "conversion" ADD COLUMN "cmg_id" TEXT;
ALTER TABLE "conversion" ADD COLUMN "xenium_id" INTEGER;

-- Poller cursor/retry/lock state for CMG synchronization
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

CREATE UNIQUE INDEX "cmg_sync_retry_poller_name_cmg_id_key"
ON "cmg_sync_retry"("poller_name", "cmg_id");

CREATE TABLE "cmg_sync_process_lock" (
    "process_name" VARCHAR(50) NOT NULL,
    "locked_by" VARCHAR(200),
    "lock_expires_at" TIMESTAMP(3),
    "last_started_at" TIMESTAMP(3),
    "last_completed_at" TIMESTAMP(3),
    "updated_at" TIMESTAMP(3) NOT NULL,
    CONSTRAINT "cmg_sync_process_lock_pkey" PRIMARY KEY ("process_name")
);

-- Poller cursor/retry/lock state for Xenium synchronization
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

CREATE UNIQUE INDEX "xenium_sync_retry_poller_name_xenium_id_key"
ON "xenium_sync_retry"("poller_name", "xenium_id");

CREATE TABLE "xenium_sync_process_lock" (
    "process_name" VARCHAR(50) NOT NULL,
    "locked_by" VARCHAR(200),
    "lock_expires_at" TIMESTAMP(3),
    "last_started_at" TIMESTAMP(3),
    "last_completed_at" TIMESTAMP(3),
    "updated_at" TIMESTAMP(3) NOT NULL,
    CONSTRAINT "xenium_sync_process_lock_pkey" PRIMARY KEY ("process_name")
);

-- Legacy CMG access type values still represented in migrated data
ALTER TYPE "access_type" ADD VALUE IF NOT EXISTS 'SLATE_PROJECT';
ALTER TYPE "access_type" ADD VALUE IF NOT EXISTS 'DCWAN';

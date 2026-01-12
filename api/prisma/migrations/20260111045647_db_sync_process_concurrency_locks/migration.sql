-- CreateTable
CREATE TABLE "cmg_sync_process_lock" (
    "process_name" VARCHAR(50) NOT NULL,
    "locked_by" VARCHAR(200),
    "lock_expires_at" TIMESTAMP(3),
    "last_started_at" TIMESTAMP(3),
    "last_completed_at" TIMESTAMP(3),
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "cmg_sync_process_lock_pkey" PRIMARY KEY ("process_name")
);

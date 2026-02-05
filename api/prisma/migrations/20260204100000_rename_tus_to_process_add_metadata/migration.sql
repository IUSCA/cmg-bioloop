-- Rename tus_id to process_id
ALTER TABLE "dataset_upload_log" RENAME COLUMN "tus_id" TO "process_id";

-- Add metadata column
ALTER TABLE "dataset_upload_log" ADD COLUMN "metadata" JSONB;

-- Drop old columns (move data to metadata first)
UPDATE "dataset_upload_log" 
SET "metadata" = jsonb_build_object(
  'failure_reason', "failure_reason",
  'file_path', "file_path",
  'file_size', "file_size"::text
)
WHERE "failure_reason" IS NOT NULL OR "file_path" IS NOT NULL OR "file_size" IS NOT NULL;

-- Now drop the columns
ALTER TABLE "dataset_upload_log" DROP COLUMN IF EXISTS "file_path";
ALTER TABLE "dataset_upload_log" DROP COLUMN IF EXISTS "file_size";
ALTER TABLE "dataset_upload_log" DROP COLUMN IF EXISTS "failure_reason";

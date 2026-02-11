-- Refactor upload and import logs to link directly to datasets
-- Move create_method from dataset_audit to dataset table
-- This simplifies the schema and removes the audit_log intermediary

-- Step 1: Add create_method column to dataset table
ALTER TABLE "dataset" ADD COLUMN IF NOT EXISTS "create_method" "DATASET_CREATE_METHOD";

-- Step 2: Transfer create_method values from dataset_audit to dataset
-- For each dataset, find the audit log with a create_method and copy it to the dataset
UPDATE "dataset" d
SET "create_method" = da."create_method"
FROM "dataset_audit" da
WHERE da."dataset_id" = d."id"
  AND da."create_method" IS NOT NULL
  AND d."create_method" IS NULL;

-- Step 3: Add dataset_id to dataset_upload_log (temporarily nullable)
ALTER TABLE "dataset_upload_log" ADD COLUMN IF NOT EXISTS "dataset_id" INTEGER;

-- Step 4: Populate dataset_id in dataset_upload_log from existing audit_log relations
UPDATE "dataset_upload_log" dul
SET "dataset_id" = da."dataset_id"
FROM "dataset_audit" da
WHERE dul."audit_log_id" = da."id"
  AND dul."dataset_id" IS NULL;

-- Step 5: Add dataset_id to dataset_import_log (temporarily nullable)
ALTER TABLE "dataset_import_log" ADD COLUMN IF NOT EXISTS "dataset_id" INTEGER;

-- Step 6: Populate dataset_id in dataset_import_log from existing audit_log relations
UPDATE "dataset_import_log" dil
SET "dataset_id" = da."dataset_id"
FROM "dataset_audit" da
WHERE dil."audit_log_id" = da."id"
  AND dil."dataset_id" IS NULL;

-- Step 7: Make dataset_id NOT NULL and add foreign key constraints
-- First, verify all records have dataset_id populated
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM "dataset_upload_log" WHERE "dataset_id" IS NULL) THEN
    RAISE EXCEPTION 'Some dataset_upload_log records have NULL dataset_id. Migration cannot proceed.';
  END IF;
  
  IF EXISTS (SELECT 1 FROM "dataset_import_log" WHERE "dataset_id" IS NULL) THEN
    RAISE EXCEPTION 'Some dataset_import_log records have NULL dataset_id. Migration cannot proceed.';
  END IF;
END $$;

-- Make dataset_id NOT NULL
ALTER TABLE "dataset_upload_log" ALTER COLUMN "dataset_id" SET NOT NULL;
ALTER TABLE "dataset_import_log" ALTER COLUMN "dataset_id" SET NOT NULL;

-- Add foreign key constraints
ALTER TABLE "dataset_upload_log" 
  ADD CONSTRAINT "dataset_upload_log_dataset_id_fkey" 
  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "dataset_import_log" 
  ADD CONSTRAINT "dataset_import_log_dataset_id_fkey" 
  FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- Step 8: Create indexes for performance
CREATE INDEX IF NOT EXISTS "dataset_upload_log_dataset_id_idx" ON "dataset_upload_log"("dataset_id");
CREATE INDEX IF NOT EXISTS "dataset_import_log_dataset_id_idx" ON "dataset_import_log"("dataset_id");

-- Step 9: Drop the old audit_log_id constraints and columns
-- First drop the foreign key constraints
ALTER TABLE "dataset_upload_log" DROP CONSTRAINT IF EXISTS "dataset_upload_log_audit_log_id_fkey";
ALTER TABLE "dataset_import_log" DROP CONSTRAINT IF EXISTS "dataset_import_log_audit_log_id_fkey";

-- Drop the unique constraints
ALTER TABLE "dataset_upload_log" DROP CONSTRAINT IF EXISTS "dataset_upload_log_audit_log_id_key";
ALTER TABLE "dataset_import_log" DROP CONSTRAINT IF EXISTS "dataset_import_log_audit_log_id_key";

-- Drop the columns
ALTER TABLE "dataset_upload_log" DROP COLUMN IF EXISTS "audit_log_id";
ALTER TABLE "dataset_import_log" DROP COLUMN IF EXISTS "audit_log_id";

-- Step 10: Remove create_method from dataset_audit and drop the unique constraint
ALTER TABLE "dataset_audit" DROP CONSTRAINT IF EXISTS "dataset_audit_dataset_id_create_method_key";
ALTER TABLE "dataset_audit" DROP COLUMN IF EXISTS "create_method";

-- Migration complete
-- Audit logs are now independent of upload/import logs
-- create_method is now stored directly on the dataset table

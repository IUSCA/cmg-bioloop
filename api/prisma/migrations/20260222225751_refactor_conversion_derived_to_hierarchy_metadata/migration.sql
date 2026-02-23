-- Add metadata JSON column to dataset_hierarchy table
ALTER TABLE "dataset_hierarchy" ADD COLUMN IF NOT EXISTS "metadata" JSONB;

-- Drop conversion_derived_dataset table (foreign keys will cascade)
DROP TABLE IF EXISTS "conversion_derived_dataset";

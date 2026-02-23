-- Add metadata JSON column to analysis_type table
ALTER TABLE "analysis_type" ADD COLUMN IF NOT EXISTS "metadata" JSONB;

-- Add metadata JSON column to workflow table
ALTER TABLE "workflow" ADD COLUMN IF NOT EXISTS "metadata" JSONB;

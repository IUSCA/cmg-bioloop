-- Script to fix existing analysis_type extensions that don't start with a dot
-- Run this manually if you already have data in analysis_type table

-- Update existing extensions to ensure they all start with a dot
UPDATE "analysis_type"
SET extension = '.' || extension
WHERE extension NOT LIKE '.%' AND extension != '';

-- Verify the fix
SELECT id, name, extension FROM "analysis_type" ORDER BY name;

-- Align project.xenium_id type with Xenium source project.id (UUID/text).
ALTER TABLE "project"
ALTER COLUMN "xenium_id" TYPE TEXT
USING "xenium_id"::TEXT;

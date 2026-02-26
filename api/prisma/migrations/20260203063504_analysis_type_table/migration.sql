/*
  Warnings:

  - A unique constraint covering the columns `[name,extension]` on the table `analysis_type` will be added. If there are existing duplicate values, this will fail.

*/
-- Drop existing index if present (may have been created with a different definition in a prior migration)
DROP INDEX IF EXISTS "analysis_type_name_extension_key";

-- CreateIndex
CREATE UNIQUE INDEX "analysis_type_name_extension_key" ON "analysis_type"("name", "extension");

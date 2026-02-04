/*
  Warnings:

  - A unique constraint covering the columns `[name,extension]` on the table `analysis_type` will be added. If there are existing duplicate values, this will fail.

*/
-- CreateIndex
CREATE UNIQUE INDEX "analysis_type_name_extension_key" ON "analysis_type"("name", "extension");

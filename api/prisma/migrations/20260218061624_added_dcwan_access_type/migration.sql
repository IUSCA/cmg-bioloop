/*
  Warnings:

  - You are about to drop the column `directory_name` on the `dataset_upload_log` table. All the data in the column will be lost.
  - You are about to drop the column `selection_mode` on the `dataset_upload_log` table. All the data in the column will be lost.

*/
-- AlterEnum
-- This migration adds more than one value to an enum.
-- With PostgreSQL versions 11 and earlier, this is not possible
-- in a single migration. This can be worked around by creating
-- multiple migrations, each migration adding only one value to
-- the enum.


ALTER TYPE "access_type" ADD VALUE 'SLATE_PROJECT';
ALTER TYPE "access_type" ADD VALUE 'DCWAN';

-- AlterTable
ALTER TABLE "dataset_upload_log" DROP COLUMN "directory_name",
DROP COLUMN "selection_mode";

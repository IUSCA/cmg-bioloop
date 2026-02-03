/*
  Warnings:

  - You are about to drop the `file_upload_log` table. If the table is not empty, all the data it contains will be lost.

*/
-- AlterEnum
ALTER TYPE "upload_status" ADD VALUE 'PERMANENTLY_FAILED';

-- DropForeignKey
ALTER TABLE "file_upload_log" DROP CONSTRAINT "file_upload_log_dataset_upload_log_id_fkey";

-- AlterTable
ALTER TABLE "dataset_upload_log" ADD COLUMN     "directory_name" TEXT,
ADD COLUMN     "failure_reason" TEXT,
ADD COLUMN     "file_path" TEXT,
ADD COLUMN     "file_size" BIGINT,
ADD COLUMN     "retry_count" INTEGER NOT NULL DEFAULT 0,
ADD COLUMN     "selection_mode" TEXT,
ADD COLUMN     "tus_id" TEXT;

-- DropTable
DROP TABLE "file_upload_log";

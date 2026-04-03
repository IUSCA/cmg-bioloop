-- DropIndex
DROP INDEX "dataset_upload_log_dataset_id_key";

-- AlterTable
ALTER TABLE "dataset" ADD COLUMN     "file_type" TEXT;

-- CreateIndex
CREATE INDEX "dataset_upload_log_dataset_id_idx" ON "dataset_upload_log"("dataset_id");

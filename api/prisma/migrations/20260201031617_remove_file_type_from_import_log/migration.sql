-- Drop file_type, genome_type, genome_value from dataset_import_log
-- These fields should be stored in dataset.metadata.file_type and dataset.genomic_details instead

-- AlterTable
ALTER TABLE "dataset_import_log" DROP COLUMN "file_type",
DROP COLUMN "genome_type",
DROP COLUMN "genome_value";

-- AlterTable
ALTER TABLE "dataset_genomic_attributes" ALTER COLUMN "genome_type" DROP NOT NULL,
ALTER COLUMN "genome_value" DROP NOT NULL;

-- AlterTable
ALTER TABLE "genome_browser_session" ALTER COLUMN "title" DROP NOT NULL,
ALTER COLUMN "genome" DROP NOT NULL,
ALTER COLUMN "genome_type" DROP NOT NULL,
ALTER COLUMN "user_id" DROP NOT NULL;

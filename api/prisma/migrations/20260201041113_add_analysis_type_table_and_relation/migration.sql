-- CreateTable
CREATE TABLE "analysis_type" (
    "id" SERIAL NOT NULL,
    "name" TEXT NOT NULL,
    "extension" TEXT NOT NULL,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analysis_type_pkey" PRIMARY KEY ("id")
);

-- CreateIndex (case-insensitive composite unique on name and extension)
CREATE UNIQUE INDEX "analysis_type_name_extension_key" ON "analysis_type"(UPPER("name"), UPPER("extension"));

-- AlterTable
ALTER TABLE "dataset" ADD COLUMN "analysis_type_id" INTEGER;

-- AddForeignKey
ALTER TABLE "dataset" ADD CONSTRAINT "dataset_analysis_type_id_fkey" FOREIGN KEY ("analysis_type_id") REFERENCES "analysis_type"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- Seed analysis_type table with CMG legacy data (only if table is empty)
INSERT INTO "analysis_type" ("name", "extension")
SELECT * FROM (VALUES
('FASTQ', '.fastq'),
('BAM', '.bam'),
('BIGWIG', '.bw'),
('VCF', '.vcf'),
('IMAGE_HE', '.tif'),
('IMAGE_CYT', '.tif'),
('WEB_SUMMARY', '.html'),
('CLOUPE', '.cloupe'),
('FASTA', '.fa'),
('NEXTCLADE', '.xlsx'),
('CRAM', '.cram'),
('SPACERANGER', '.tar.gz'),
('CELLRANGER', '.gz'),
('UNALINGED-BAM', '.bam')
) AS v(name, extension)
WHERE NOT EXISTS (SELECT 1 FROM "analysis_type");

-- Update existing extensions to ensure they all start with a dot
UPDATE "analysis_type"
SET extension = '.' || extension
WHERE extension NOT LIKE '.%' AND extension != '';

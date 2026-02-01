-- CreateTable
CREATE TABLE "analysis_type" (
    "id" SERIAL NOT NULL,
    "name" TEXT NOT NULL,
    "extension" TEXT NOT NULL,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analysis_type_pkey" PRIMARY KEY ("id")
);

-- CreateIndex (case-insensitive)
CREATE UNIQUE INDEX "analysis_type_name_key" ON "analysis_type"(UPPER("name"));

-- AlterTable
ALTER TABLE "dataset" ADD COLUMN "analysis_type_id" INTEGER;

-- AddForeignKey
ALTER TABLE "dataset" ADD CONSTRAINT "dataset_analysis_type_id_fkey" FOREIGN KEY ("analysis_type_id") REFERENCES "analysis_type"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- Seed analysis_type table with CMG legacy data
INSERT INTO "analysis_type" ("name", "extension") VALUES
('FASTQ', 'fastq'),
('BAM', 'bam'),
('BIGWIG', 'bw'),
('VCF', 'vcf'),
('IMAGE_HE', '.tif'),
('IMAGE_CYT', '.tif'),
('WEB_SUMMARY', '.html'),
('CLOUPE', '.cloupe'),
('FASTA', '.fa .fasta'),
('NEXTCLADE', '.xlsx .csv'),
('CRAM', '.cram'),
('SPACERANGER', 'tar.gz'),
('CELLRANGER', ',gz'),
('UNALINGED-BAM', '.bam');

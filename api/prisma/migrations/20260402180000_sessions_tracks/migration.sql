-- Sessions/Tracks

-- Track entities
CREATE TABLE "track" (
    "id" SERIAL NOT NULL,
    "name" TEXT NOT NULL,
    "dataset_file_id" INTEGER NOT NULL,
    "color" TEXT,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "track_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "track_dataset_file_id_key" ON "track"("dataset_file_id");
ALTER TABLE "track"
ADD CONSTRAINT "track_dataset_file_id_fkey"
FOREIGN KEY ("dataset_file_id") REFERENCES "dataset_file"("id")
ON DELETE CASCADE ON UPDATE CASCADE;

-- Genome browser sessions
CREATE TABLE "genome_browser_session" (
    "id" SERIAL NOT NULL,
    "title" TEXT,
    "genome" TEXT,
    "genome_type" TEXT,
    "user_id" INTEGER,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "access_count" INTEGER NOT NULL DEFAULT 0,
    "is_public" BOOLEAN NOT NULL DEFAULT false,
    "metadata" JSONB,
    CONSTRAINT "genome_browser_session_pkey" PRIMARY KEY ("id")
);

ALTER TABLE "genome_browser_session"
ADD CONSTRAINT "genome_browser_session_user_id_fkey"
FOREIGN KEY ("user_id") REFERENCES "user"("id")
ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "session_track" (
    "id" SERIAL NOT NULL,
    "session_id" INTEGER NOT NULL,
    "track_id" INTEGER NOT NULL,
    "color" TEXT,
    "title" TEXT,
    "order" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "session_track_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "session_track_session_id_track_id_key" ON "session_track"("session_id", "track_id");
ALTER TABLE "session_track"
ADD CONSTRAINT "session_track_session_id_fkey"
FOREIGN KEY ("session_id") REFERENCES "genome_browser_session"("id")
ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "session_track"
ADD CONSTRAINT "session_track_track_id_fkey"
FOREIGN KEY ("track_id") REFERENCES "track"("id")
ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "session_workflow" (
    "session_id" INTEGER NOT NULL,
    "workflow_id" TEXT NOT NULL,
    "initiator_id" INTEGER,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "session_workflow_pkey" PRIMARY KEY ("session_id", "workflow_id")
);

ALTER TABLE "session_workflow"
ADD CONSTRAINT "session_workflow_session_id_fkey"
FOREIGN KEY ("session_id") REFERENCES "genome_browser_session"("id")
ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "session_workflow"
ADD CONSTRAINT "session_workflow_initiator_id_fkey"
FOREIGN KEY ("initiator_id") REFERENCES "user"("id")
ON DELETE SET NULL ON UPDATE CASCADE;

-- Analysis type support
CREATE TABLE "analysis_type" (
    "id" SERIAL NOT NULL,
    "name" TEXT NOT NULL,
    "extension" TEXT NOT NULL,
    "metadata" JSONB,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "analysis_type_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "analysis_type_name_extension_key" ON "analysis_type"("name", "extension");

ALTER TABLE "dataset"
ADD COLUMN "analysis_type_id" INTEGER;
ALTER TABLE "dataset"
ADD CONSTRAINT "dataset_analysis_type_id_fkey"
FOREIGN KEY ("analysis_type_id") REFERENCES "analysis_type"("id")
ON DELETE SET NULL ON UPDATE CASCADE;

CREATE TABLE "dataset_genomic_attributes" (
    "dataset_id" INTEGER NOT NULL,
    "genome_type" TEXT,
    "genome_value" TEXT,
    CONSTRAINT "dataset_genomic_attributes_pkey" PRIMARY KEY ("dataset_id")
);
ALTER TABLE "dataset_genomic_attributes"
ADD CONSTRAINT "dataset_genomic_attributes_dataset_id_fkey"
FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id")
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "workflow"
ADD COLUMN IF NOT EXISTS "metadata" JSONB;

-- Seed + normalize analysis_type values
INSERT INTO "analysis_type" ("name", "extension")
VALUES
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
ON CONFLICT ("name", "extension") DO NOTHING;

UPDATE "analysis_type"
SET "extension" = '.' || "extension"
WHERE "extension" NOT LIKE '.%' AND "extension" <> '';

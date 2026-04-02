-- Conversions

CREATE TYPE "argument_value_type" AS ENUM ('STRING', 'NUMBER', 'BOOLEAN');

CREATE TABLE "cmd_line_program" (
    "id" SERIAL NOT NULL,
    "name" TEXT NOT NULL,
    "executable_path" TEXT NOT NULL,
    "executable_directory" TEXT,
    "allow_additional_args" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "cmd_line_program_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "cmd_line_program_name_key" ON "cmd_line_program"("name");
CREATE UNIQUE INDEX "cmd_line_program_name_executable_path_key" ON "cmd_line_program"("name", "executable_path");

CREATE TABLE "conversion_definition" (
    "id" SERIAL NOT NULL,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "name" TEXT NOT NULL,
    "description" TEXT,
    "enabled" BOOLEAN NOT NULL DEFAULT false,
    "author_id" INTEGER NOT NULL,
    "dataset_types" TEXT[],
    "tags" TEXT[],
    "program_id" INTEGER NOT NULL,
    "output_directory" TEXT,
    "logs_directory" TEXT,
    "capture_logs" BOOLEAN NOT NULL DEFAULT false,
    CONSTRAINT "conversion_definition_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "conversion_definition_name_key" ON "conversion_definition"("name");

CREATE TABLE "dynamic_variable" (
    "name" TEXT NOT NULL,
    "description" TEXT,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "dynamic_variable_pkey" PRIMARY KEY ("name")
);

CREATE TABLE "argument" (
    "id" SERIAL NOT NULL,
    "name" TEXT,
    "value_type" "argument_value_type" NOT NULL,
    "allowed_values" TEXT[],
    "is_required" BOOLEAN NOT NULL DEFAULT false,
    "default_value" TEXT,
    "is_flag" BOOLEAN NOT NULL DEFAULT false,
    "description" TEXT,
    "min_value" DOUBLE PRECISION,
    "max_value" DOUBLE PRECISION,
    "min_length" INTEGER,
    "max_length" INTEGER,
    "position" INTEGER,
    "dynamic_variable_name" TEXT,
    "program_id" INTEGER NOT NULL,
    CONSTRAINT "argument_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "conversion" (
    "id" SERIAL NOT NULL,
    "initiated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "definition_id" INTEGER NOT NULL,
    "workflow_id" TEXT,
    "dataset_id" INTEGER,
    "initiator_id" INTEGER,
    "additional_args" JSONB,
    CONSTRAINT "conversion_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "argument_value" (
    "id" SERIAL NOT NULL,
    "argument_id" INTEGER NOT NULL,
    "conversion_id" INTEGER NOT NULL,
    "value" TEXT,
    CONSTRAINT "argument_value_pkey" PRIMARY KEY ("id")
);

ALTER TABLE "conversion_definition"
ADD CONSTRAINT "conversion_definition_program_id_fkey"
FOREIGN KEY ("program_id") REFERENCES "cmd_line_program"("id")
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "conversion_definition"
ADD CONSTRAINT "conversion_definition_author_id_fkey"
FOREIGN KEY ("author_id") REFERENCES "user"("id")
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "argument"
ADD CONSTRAINT "argument_dynamic_variable_name_fkey"
FOREIGN KEY ("dynamic_variable_name") REFERENCES "dynamic_variable"("name")
ON DELETE SET NULL ON UPDATE CASCADE;

ALTER TABLE "argument"
ADD CONSTRAINT "argument_program_id_fkey"
FOREIGN KEY ("program_id") REFERENCES "cmd_line_program"("id")
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "conversion"
ADD CONSTRAINT "conversion_dataset_id_fkey"
FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id")
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "conversion"
ADD CONSTRAINT "conversion_definition_id_fkey"
FOREIGN KEY ("definition_id") REFERENCES "conversion_definition"("id")
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "conversion"
ADD CONSTRAINT "conversion_initiator_id_fkey"
FOREIGN KEY ("initiator_id") REFERENCES "user"("id")
ON DELETE SET NULL ON UPDATE CASCADE;

ALTER TABLE "argument_value"
ADD CONSTRAINT "argument_value_argument_id_fkey"
FOREIGN KEY ("argument_id") REFERENCES "argument"("id")
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "argument_value"
ADD CONSTRAINT "argument_value_conversion_id_fkey"
FOREIGN KEY ("conversion_id") REFERENCES "conversion"("id")
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "dataset_hierarchy"
ADD COLUMN IF NOT EXISTS "metadata" JSONB;

ALTER TABLE "workflow"
ADD COLUMN IF NOT EXISTS "metadata" JSONB;

ALTER TYPE "DATASET_CREATE_METHOD" ADD VALUE IF NOT EXISTS 'CONVERSION';

UPDATE "argument"
SET
    "name" = '--use-bases-mask',
    "value_type" = 'STRING',
    "is_flag" = FALSE,
    "description" = 'Override the bases mask string for the run'
WHERE "name" = '--uses-bases-mask';

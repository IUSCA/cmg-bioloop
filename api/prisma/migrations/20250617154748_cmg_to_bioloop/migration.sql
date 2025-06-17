-- AlterTable
ALTER TABLE "dataset" ADD COLUMN     "cmg_id" TEXT;

-- AlterTable
ALTER TABLE "dataset_audit" ADD COLUMN     "description" TEXT;

-- CreateTable
CREATE TABLE "import_log" (
    "id" SERIAL NOT NULL,
    "initiated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "user_id" INTEGER,

    CONSTRAINT "import_log_pkey" PRIMARY KEY ("id")
);

-- AddForeignKey
ALTER TABLE "import_log" ADD CONSTRAINT "import_log_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE SET NULL ON UPDATE CASCADE;

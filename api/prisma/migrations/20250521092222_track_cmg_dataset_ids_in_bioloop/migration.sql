/*
  Warnings:

  - A unique constraint covering the columns `[cmg_id]` on the table `dataset` will be added. If there are existing duplicate values, this will fail.
  - Added the required column `cmg_id` to the `dataset` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "dataset" ADD COLUMN     "cmg_id" TEXT NOT NULL;

-- CreateTable
CREATE TABLE "import_log" (
    "id" SERIAL NOT NULL,
    "initiated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "user_id" INTEGER,

    CONSTRAINT "import_log_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "dataset_cmg_id_key" ON "dataset"("cmg_id");

-- AddForeignKey
ALTER TABLE "import_log" ADD CONSTRAINT "import_log_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE SET NULL ON UPDATE CASCADE;

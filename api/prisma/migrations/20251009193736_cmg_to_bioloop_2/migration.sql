-- AlterTable
ALTER TABLE "conversion" ALTER COLUMN "dataset_id" DROP NOT NULL;

-- AlterTable
ALTER TABLE "project" ADD COLUMN     "cmg_id" TEXT;

-- AlterTable
ALTER TABLE "user" ADD COLUMN     "cmg_id" VARCHAR(100);

/*
  Warnings:

  - You are about to drop the column `staging_completed` on the `genome_browser_session` table. All the data in the column will be lost.
  - You are about to drop the column `staging_requested` on the `genome_browser_session` table. All the data in the column will be lost.
  - You are about to drop the column `staging_requested_by` on the `genome_browser_session` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "genome_browser_session" DROP COLUMN "staging_completed",
DROP COLUMN "staging_requested",
DROP COLUMN "staging_requested_by",
ADD COLUMN     "metadata" JSONB;

-- CreateTable
CREATE TABLE "session_workflow" (
    "session_id" INTEGER NOT NULL,
    "workflow_id" TEXT NOT NULL,
    "initiator_id" INTEGER,
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "session_workflow_pkey" PRIMARY KEY ("session_id","workflow_id")
);

-- AddForeignKey
ALTER TABLE "session_workflow" ADD CONSTRAINT "session_workflow_session_id_fkey" FOREIGN KEY ("session_id") REFERENCES "genome_browser_session"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "session_workflow" ADD CONSTRAINT "session_workflow_initiator_id_fkey" FOREIGN KEY ("initiator_id") REFERENCES "user"("id") ON DELETE SET NULL ON UPDATE CASCADE;

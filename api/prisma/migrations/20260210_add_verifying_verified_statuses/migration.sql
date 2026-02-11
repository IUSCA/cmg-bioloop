-- Add VERIFYING and VERIFIED statuses to upload_status enum
-- These statuses are used by the async upload verification system

-- Add VERIFYING status (verification task is running)
ALTER TYPE "upload_status" ADD VALUE IF NOT EXISTS 'VERIFYING' AFTER 'UPLOADED';

-- Add VERIFIED status (verification complete, ready to trigger workflow)
ALTER TYPE "upload_status" ADD VALUE IF NOT EXISTS 'VERIFIED' AFTER 'VERIFYING';

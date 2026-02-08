/**
 * Generic Upload Routes
 *
 * Mounts the TUS upload server and provides supporting endpoints
 * for querying upload status and managing uploads.
 */

const express = require('express');
const asyncHandler = require('@/middleware/asyncHandler');
const { accessControl, authenticate } = require('@/middleware/auth');
const prisma = require('@/db');
const uploadService = require('@/services/upload');
const logger = require('@/services/logger');
const constants = require('@/constants');

console.log('===== uploads.js module loading =====');

const router = express.Router();
const isPermittedTo = accessControl('uploads');

// Test endpoint to verify routing
router.get('/test', (req, res) => {
  console.log('Test endpoint hit!');
  logger.info('Test endpoint hit!');
  res.json({ message: 'Uploads router is working!' });
});

console.log('Registered GET /test route');

// TUS server is now mounted directly in app.js BEFORE this router
// This avoids issues with Express middleware modifying req/res objects
logger.info('TUS server is mounted in app.js at /uploads/files');

/**
 * Get upload status for an entity
 * GET /api/uploads/status/:entityType/:entityId
 */
router.get(
  '/status/:entityType/:entityId',
  isPermittedTo('read'),
  asyncHandler(async (req, res) => {
    const { entityType, entityId } = req.params;

    // For now, only support datasets
    // Future: add other entity types
    if (entityType !== 'dataset') {
      return res.status(400).json({ error: 'Unsupported entity type' });
    }

    const datasetId = parseInt(entityId, 10);

    // Get upload log
    const uploadLog = await prisma.dataset_upload_log.findFirst({
      where: {
        audit_log: {
          dataset_id: datasetId,
        },
      },
      include: {
        audit_log: {
          include: {
            dataset: {
              select: {
                id: true,
                name: true,
                type: true,
              },
            },
          },
        },
      },
    });

    if (!uploadLog) {
      return res.status(404).json({ error: 'Upload not found' });
    }

    res.json({
      status: uploadLog.status,
      process_id: uploadLog.process_id,
      retry_count: uploadLog.retry_count,
      metadata: uploadLog.metadata,
      updated_at: uploadLog.updated_at,
      dataset: uploadLog.audit_log.dataset,
    });
  }),
);

/**
 * Helper endpoints for retry jobs
 */

// Get stalled uploads (UPLOADED, VERIFYING, or VERIFIED - need processing)
router.get(
  '/stalled',
  authenticate, // Service-to-service endpoint - just needs valid token
  asyncHandler(async (req, res) => {
    const stalledThreshold = new Date(Date.now() - 30 * 1000); // 30 second buffer to avoid race conditions

    const stalled = await prisma.dataset_upload_log.findMany({
      where: {
        status: {
          in: [
            constants.UPLOAD_STATUSES.UPLOADED,
            constants.UPLOAD_STATUSES.VERIFYING,
            constants.UPLOAD_STATUSES.VERIFIED,
          ],
        },
        updated_at: { lt: stalledThreshold },
      },
      include: {
        audit_log: {
          include: { dataset: true },
        },
      },
    });

    res.json({
      uploads: stalled.map((u) => ({
        dataset_id: u.audit_log.dataset.id,
        dataset_name: u.audit_log.dataset.name,
        uploaded_at: u.updated_at,
      })),
    });
  }),
);

// Get failed uploads (with retry count filter)
router.get(
  '/failed',
  authenticate, // Service-to-service endpoint - just needs valid token
  asyncHandler(async (req, res) => {
    const { max_retry_count = 2, max_age_hours = 72 } = req.query;
    const cutoffDate = new Date(Date.now() - max_age_hours * 60 * 60 * 1000);

    const failed = await prisma.dataset_upload_log.findMany({
      where: {
        status: 'PROCESSING_FAILED',
        retry_count: { lte: parseInt(max_retry_count, 10) },
        updated_at: { gte: cutoffDate },
      },
      include: {
        audit_log: {
          include: { dataset: true },
        },
      },
    });

    res.json({
      uploads: failed.map((u) => ({
        dataset_id: u.audit_log.dataset.id,
        dataset_name: u.audit_log.dataset.name,
        retry_count: u.retry_count || 0,
        last_error: u.metadata?.failure_reason || null,
      })),
    });
  }),
);

// Get expired uploads (UPLOADING > X days)
router.get(
  '/expired',
  authenticate, // Service-to-service endpoint - just needs valid token
  asyncHandler(async (req, res) => {
    const { status = 'UPLOADING', age_days = 7 } = req.query;
    const cutoffDate = new Date(Date.now() - age_days * 24 * 60 * 60 * 1000);

    const expired = await prisma.dataset_upload_log.findMany({
      where: {
        status,
        updated_at: { lt: cutoffDate },
      },
      include: {
        audit_log: {
          include: { dataset: true },
        },
      },
    });

    res.json({
      uploads: expired.map((u) => ({
        dataset_id: u.audit_log.dataset.id,
        dataset_name: u.audit_log.dataset.name,
        status: u.status,
        age_days: Math.floor((Date.now() - u.updated_at) / (24 * 60 * 60 * 1000)),
      })),
    });
  }),
);

// Get all process IDs (for orphaned file detection)
router.get(
  '/all-process-ids',
  authenticate, // Service-to-service endpoint - just needs valid token
  asyncHandler(async (req, res) => {
    const uploads = await prisma.dataset_upload_log.findMany({
      select: { process_id: true },
      where: { process_id: { not: null } },
    });

    res.json({
      process_ids: uploads.map((u) => u.process_id).filter(Boolean),
    });
  }),
);

// Get uploads by status (for monitoring)
router.get(
  '/by-status',
  authenticate, // Service-to-service endpoint - just needs valid token
  asyncHandler(async (req, res) => {
    const { statuses = [] } = req.query;

    const uploads = await prisma.dataset_upload_log.findMany({
      where: {
        status: { in: statuses },
      },
      include: {
        audit_log: {
          include: { dataset: true },
        },
      },
    });

    res.json({
      uploads: uploads.map((u) => ({
        dataset_id: u.audit_log.dataset.id,
        dataset_name: u.audit_log.dataset.name,
        origin_path: u.audit_log.dataset.origin_path,
        status: u.status,
      })),
    });
  }),
);

// Update upload retry count, status, and metadata
router.patch(
  '/:id',
  authenticate, // Service-to-service endpoint - just needs valid token
  asyncHandler(async (req, res) => {
    const { retry_count, status, metadata } = req.body;

    // Get existing metadata to merge
    const existing = await prisma.dataset_upload_log.findUnique({
      where: { id: parseInt(req.params.id, 10) },
      select: { metadata: true },
    });

    const mergedMetadata = metadata
      ? { ...(existing?.metadata || {}), ...metadata }
      : undefined;

    const updated = await prisma.dataset_upload_log.update({
      where: { id: parseInt(req.params.id, 10) },
      data: {
        ...(retry_count !== undefined && { retry_count }),
        ...(status && { status }),
        ...(mergedMetadata && { metadata: mergedMetadata }),
      },
    });

    res.json(updated);
  }),
);

console.log('===== uploads.js exporting router =====');

module.exports = router;

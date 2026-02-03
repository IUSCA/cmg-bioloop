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
      tus_id: uploadLog.tus_id,
      file_path: uploadLog.file_path,
      file_size: uploadLog.file_size ? String(uploadLog.file_size) : null,
      selection_mode: uploadLog.selection_mode,
      directory_name: uploadLog.directory_name,
      retry_count: uploadLog.retry_count,
      failure_reason: uploadLog.failure_reason,
      updated_at: uploadLog.updated_at,
      dataset: uploadLog.audit_log.dataset,
    });
  })
);

/**
 * Helper endpoints for retry jobs
 */

// Get stalled uploads (UPLOADED but workflow not started)
router.get(
  '/stalled',
  isPermittedTo('read'),
  asyncHandler(async (req, res) => {
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
    
    const stalled = await prisma.dataset_upload_log.findMany({
      where: {
        status: 'UPLOADED',
        updated_at: { lt: fiveMinutesAgo },
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
  })
);

// Get failed uploads (with retry count filter)
router.get(
  '/failed',
  isPermittedTo('read'),
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
        last_error: u.failure_reason,
      })),
    });
  })
);

// Get expired uploads (UPLOADING > X days)
router.get(
  '/expired',
  isPermittedTo('read'),
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
  })
);

// Get all TUS IDs (for orphaned file detection)
router.get(
  '/all-tus-ids',
  isPermittedTo('read'),
  asyncHandler(async (req, res) => {
    const uploads = await prisma.dataset_upload_log.findMany({
      select: { tus_id: true },
      where: { tus_id: { not: null } },
    });
    
    res.json({
      tus_ids: uploads.map((u) => u.tus_id).filter(Boolean),
    });
  })
);

// Get uploads with file paths (for missing file detection)
router.get(
  '/with-file-paths',
  isPermittedTo('read'),
  asyncHandler(async (req, res) => {
    const { statuses = [] } = req.query;
    
    const uploads = await prisma.dataset_upload_log.findMany({
      where: {
        status: { in: statuses },
        file_path: { not: null },
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
        file_path: u.file_path,
        status: u.status,
      })),
    });
  })
);

// Update upload retry count and status
router.patch(
  '/:id',
  isPermittedTo('update'),
  asyncHandler(async (req, res) => {
    const { retry_count, status, failure_reason } = req.body;
    
    const updated = await prisma.dataset_upload_log.update({
      where: { id: parseInt(req.params.id, 10) },
      data: {
        ...(retry_count !== undefined && { retry_count }),
        ...(status && { status }),
        ...(failure_reason && { failure_reason }),
      },
    });
    
    res.json(updated);
  })
);

console.log('===== uploads.js exporting router =====');

module.exports = router;

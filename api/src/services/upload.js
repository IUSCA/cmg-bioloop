/**
 * Generic TUS Upload Service
 *
 * Provides entity-agnostic file upload functionality using TUS protocol.
 * Delegates entity-specific logic to registered handlers.
 *
 * Features:
 * - Resumable uploads (up to 100GB per file)
 * - Automatic cleanup of expired uploads (7 days)
 * - Support for single files, multiple files, and directories
 * - Entity-specific business logic via handlers
 */

const { Server } = require('@tus/server');
const { FileStore } = require('@tus/file-store');
const config = require('config');
const logger = require('@/services/logger');

class UploadService {
  constructor() {
    const uploadPath = config.get('upload.path');

    logger.info(`Initializing TUS Upload Service at: ${uploadPath}`);

    this.tusServer = new Server({
      path: '/api/uploads/files', // Public path (what clients see) for correct Location headers
      maxSize: 100 * 1024 * 1024 * 1024, // 100 GB max file size
      relativeLocation: true, // Return relative paths like /api/uploads/files/{id}

      datastore: new FileStore({
        directory: uploadPath,
        // Automatically delete incomplete uploads after 7 days
        expirationPeriodInMilliseconds: 7 * 24 * 60 * 60 * 1000,
      }),

      // Hooks disabled - moved to /complete endpoint to avoid TUS response stream conflict
      // onUploadCreate: this.handleUploadCreate.bind(this),
      // onUploadFinish: this.handleUploadFinish.bind(this),
    });

    logger.info('TUS Upload Service initialized');
  }

  /**
   * Called when a new upload is created (before any bytes are uploaded)
   */
  async handleUploadCreate(req, res, upload) {
    const entityType = upload.metadata.entity_type;
    const entityId = upload.metadata.entity_id;

    logger.info(`Upload create: type=${entityType}, id=${entityId}, upload_id=${upload.id}`);

    // Validate required metadata
    if (!entityType) {
      throw new Error('Missing required metadata: entity_type');
    }
    if (!entityId) {
      throw new Error('Missing required metadata: entity_id');
    }

    // Delegate to entity-specific handler
    // Run handler in background to avoid blocking TUS response
    const handler = this.getEntityHandler(entityType);
    if (handler?.onUploadCreate) {
      handler.onUploadCreate(req, res, upload).catch((error) => {
        logger.error('Upload create handler error:', error);
        // Don't throw - let TUS complete the response
      });
    }

    logger.info(`Upload create initiated for: ${entityType}:${entityId}`);
  }

  /**
   * Called when upload completes successfully
   */
  async handleUploadFinish(req, res, upload) {
    try {
      const entityType = upload.metadata.entity_type;
      const entityId = upload.metadata.entity_id;
      const filePath = upload.storage.path;
      const fileSize = upload.size;

      logger.info(
        `Upload finish: type=${entityType}, id=${entityId}, `
        + `upload_id=${upload.id}, path=${filePath}, size=${fileSize}`,
      );

      // Delegate to entity-specific handler
      const handler = this.getEntityHandler(entityType);
      if (handler?.onUploadFinish) {
        await handler.onUploadFinish(req, res, upload, filePath, fileSize);
      } else {
        logger.warn(`No upload handler found for entity type: ${entityType}`);
      }

      logger.info(`Upload finish completed successfully for: ${entityType}:${entityId}`);
    } catch (error) {
      logger.error('Upload finish error:', error);
      throw error; // Re-throw to let TUS handle it
    }
  }

  /**
   * Get entity-specific upload handler
   */
  getEntityHandler(entityType) {
    // Registry of entity-specific handlers
    const handlers = {
      dataset: require('./upload/datasetHandler'),
      // Future: add more entity types here
      // session: require('./upload/sessionHandler'),
      // report: require('./upload/reportHandler'),
    };

    const handler = handlers[entityType];

    if (!handler) {
      logger.warn(`Unknown entity type: ${entityType}. Available: ${Object.keys(handlers).join(', ')}`);
    }

    return handler;
  }

  /**
   * Get the TUS server instance for mounting in Express
   */
  getServer() {
    return this.tusServer;
  }
}

// Export singleton instance
module.exports = new UploadService();

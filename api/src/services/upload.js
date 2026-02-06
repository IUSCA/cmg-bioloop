/**
 * Generic TUS Upload Service
 *
 * Provides entity-agnostic file upload functionality using TUS protocol.
 *
 * Features:
 * - Resumable uploads (up to 100GB per file)
 * - Automatic cleanup of expired uploads (7 days)
 * - Support for single files, multiple files, and directories
 *
 * Note: Post-upload processing is handled by explicit /complete endpoints,
 * not TUS hooks, to avoid response stream conflicts with async operations.
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
    });

    logger.info('TUS Upload Service initialized');
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

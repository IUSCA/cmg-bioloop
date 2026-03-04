/**
 * Upload Service
 *
 * Central service for upload-related utilities including TUS protocol helpers.
 */

import config from "@/config";

/**
 * Get the absolute upload service URL.
 *
 * The upload endpoint must be an absolute URL (not just a path like '/api/uploads/files')
 *
 * Why not use a service file pattern like other API calls?
 * - Standard REST API calls work fine with relative URLs (e.g., '/api/datasets')
 * - The upload protocol (TUS) is different: the server returns a Location header in POST responses
 *   pointing to the upload resource (e.g., '/api/uploads/files/abc123')
 * - The upload client library needs to resolve this Location header against the endpoint base URL
 * - If endpoint is relative, Location header resolution fails or produces incorrect URLs
 * - The upload protocol specification requires Location headers to be absolute URLs
 * - By constructing the endpoint with origin, we ensure:
 *   1. The upload client can correctly resolve Location headers from server responses
 *   2. Upload resource URLs are properly constructed for subsequent requests
 *
 * This is a protocol requirement for resumable uploads, not a code organization choice.
 * Technical details: https://github.com/tus/tus-js-client/issues/694
 *
 * @param {string} origin - The origin URL (e.g., window.location.origin)
 * @returns {string} Absolute upload service URL
 */
export function _getUploadServiceURL(origin) {
  return `${origin}${config.apiBasePath}/uploads/files`;
}

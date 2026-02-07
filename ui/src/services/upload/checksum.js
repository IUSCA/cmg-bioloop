/**
 * Upload Checksum Service
 *
 * Provides BLAKE3 manifest-based checksum computation for upload verification.
 * Uses hash-wasm for browser-compatible BLAKE3 hashing.
 */

import config from '@/config';

let blake3Fn = null;

/**
 * Load BLAKE3 hash function lazily
 * @private
 */
async function _loadBlake3() {
  if (blake3Fn) {
    return blake3Fn;
  }

  try {
    // Dynamic import to avoid loading WASM until needed
    const hashWasm = await import('hash-wasm');
    blake3Fn = hashWasm.blake3;
    return blake3Fn;
  } catch (error) {
    console.error('Failed to load hash-wasm module:', error);
    throw error;
  }
}

/**
 * Normalize file path for cross-platform consistency
 * @private
 * @param {string} path - File path to normalize
 * @returns {string} Normalized path with forward slashes
 */
function _normalizePath(path) {
  // Use forward slashes, remove leading ./
  return path.replace(/\\/g, '/').replace(/^\.\//, '');
}

/**
 * Compute BLAKE3 hash for a single file
 * @private
 * @param {File} file - File to hash
 * @param {Function} blake3 - BLAKE3 hash function from hash-wasm
 * @returns {Promise<string>} Hex hash string
 */
async function _hashFile(file, blake3) {
  const arrayBuffer = await file.arrayBuffer();
  const hash = await blake3(new Uint8Array(arrayBuffer));
  return hash;
}

/**
 * Compute BLAKE3 manifest hash for uploaded files.
 *
 * Creates a deterministic manifest string with file paths, sizes, and hashes,
 * then hashes the manifest itself for verification.
 *
 * @param {File[]} files - Array of File objects to hash
 * @param {Function} [progressCallback] - Optional callback for progress updates (0-100)
 * @returns {Promise<Object|null>} Manifest hash object or null if feature disabled/no files
 */
export async function _computeManifestHash(files, progressCallback = null) {
  // Check feature flag
  if (!config.enabledFeatures.upload_verify_checksums) {
    return null; // Feature disabled
  }

  if (!files || files.length === 0) {
    return null;
  }

  try {
    const blake3 = await _loadBlake3();
    const manifest = [];
    const totalFiles = files.length;

    // Hash each file
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      const fileHash = await _hashFile(file, blake3);

      manifest.push({
        path: _normalizePath(file.webkitRelativePath || file.name),
        size: file.size,
        hash: fileHash,
      });

      // Report progress
      if (progressCallback) {
        const progress = Math.round(((i + 1) / totalFiles) * 100);
        progressCallback(progress);
      }
    }

    // Sort by path for deterministic order
    manifest.sort((a, b) => a.path.localeCompare(b.path));

    // Create canonical manifest string
    const manifestStr = [
      'blake3-manifest-v1',
      ...manifest.map((f) => `${f.path}\t${f.size}\t${f.hash}`),
    ].join('\n');

    // Hash the manifest
    const manifestBytes = new TextEncoder().encode(manifestStr);
    const manifestHash = await blake3(manifestBytes);

    return {
      algorithm: 'blake3',
      mode: files.length === 1 ? 'single' : 'manifest-v1',
      manifest_hash: manifestHash,
      file_count: files.length,
      total_size: manifest.reduce((sum, f) => sum + f.size, 0),
      computed_at: new Date().toISOString(),
    };
  } catch (error) {
    console.error('Failed to compute manifest hash:', error);
    // Don't fail upload if checksum computation fails
    return null;
  }
}

/**
 * Check if checksum verification is enabled
 * @returns {boolean} True if feature is enabled
 */
export function _isChecksumVerificationEnabled() {
  return Boolean(config.enabledFeatures.upload_verify_checksums);
}

export default {
  _computeManifestHash,
  _isChecksumVerificationEnabled,
};

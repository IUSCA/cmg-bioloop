const { constants } = require('node:fs');
const fs = require('fs');
const express = require('express');
const {
  query,
  // param, body, checkSchema,
} = require('express-validator');
const path = require('node:path');
const createError = require('http-errors');

const config = require('config');
// eslint-disable-next-line lodash-fp/use-fp
const _ = require('lodash');
const logger = require('@/services/logger');
const asyncHandler = require('../middleware/asyncHandler');
const { accessControl } = require('../middleware/auth');

const isPermittedTo = accessControl('fs');

const router = express.Router();

/**
 * Check if a directory contains files with the specified extension
 * @param {string} dirPath - Path to the directory to check
 * @param {string} extension - File extension to look for (e.g., '.fastq.gz')
 * @returns {Promise<boolean>} - True if directory contains files with the extension
 */
function directoryContainsExtension(dirPath, extension) {
  return new Promise((resolve) => {
    if (!extension) {
      resolve(true);
      return;
    }

    fs.readdir(dirPath, { withFileTypes: true }, (err, files) => {
      if (err) {
        logger.warn('[FS] Error reading directory for extension check', {
          dirPath,
          extension,
          error: err.message,
        });
        resolve(false);
        return;
      }

      const hasMatchingFiles = files.some((file) => {
        if (file.isDirectory()) {
          return false;
        }
        return file.name.endsWith(extension);
      });

      resolve(hasMatchingFiles);
    });
  });
}

function getBaseDirKey(req) {
  return Object.keys(config.filesystem.base_dir).filter((key) => key === req.query.search_space)[0];
}

function getBaseDir(req) {
  const base_dir_key = getBaseDirKey(req);
  return config.filesystem.base_dir[base_dir_key];
}

function validatePath(req, res, next) {
  const query_path = req.query.path;

  logger.info('[FS] validatePath called', {
    query_path,
    search_space: req.query.search_space,
  });

  if (!query_path) {
    logger.warn('[FS] validatePath failed: no query_path');
    return next(createError.Forbidden());
  }

  // Preserve trailing slash information before normalization
  req.hasTrailingSlash = query_path.endsWith('/');

  let p = query_path ? path.normalize(query_path) : null;
  if (!p || !path.isAbsolute(p)) {
    logger.warn('[FS] validatePath failed: path not absolute', {
      query_path,
      normalized: p,
    });
    res.status(400).send('Invalid path');
    return;
  }

  p = path.resolve(p);

  const base_dir = getBaseDir(req);
  logger.info('[FS] validatePath checking base_dir', {
    resolved_path: p,
    base_dir,
    starts_with_base: p.startsWith(base_dir),
    had_trailing_slash: req.hasTrailingSlash,
  });

  if (!p.startsWith(base_dir)) {
    logger.warn('[FS] validatePath failed: path outside base_dir', {
      resolved_path: p,
      base_dir,
    });
    res.status(403).send('Forbidden');
    return;
  }

  req.query.path = p;
  logger.info('[FS] validatePath passed', {
    final_path: p,
    had_trailing_slash: req.hasTrailingSlash,
  });
  next();
}

const get_mount_dir = (req) => {
  const base_dir_key = getBaseDirKey(req);
  return config.filesystem.mount_dir[base_dir_key];
};

const get_mounted_search_dir = (req) => {
  const base_dir = getBaseDir(req);
  const path_prefix = `${base_dir}/`;

  const query_path = req.query.path.slice(req.query.path.indexOf(path_prefix)
      + path_prefix.length);
  const mount_dir = get_mount_dir(req);
  return path.join(mount_dir, query_path);
};

router.get(
  '/',
  validatePath,
  isPermittedTo('read'),
  query('dirs_only').default(false),
  query('search_space').optional().trim().isLength({ min: 1 }),
  query('extension').optional().trim(),
  asyncHandler(async (req, res, next) => {
    const { dirs_only, path: query_path, extension } = req.query;

    logger.info('[FS] Request received', {
      query_path,
      dirs_only,
      search_space: req.query.search_space,
      extension,
      user: req.user?.username,
    });

    // TEMPORARY: Hardcoded response for testing
    if (config.get('mode') === 'docker') {
      logger.info('[FS] Returning hardcoded response');
      res.json([{
        name: 'bigWig_h3k4me3_hg19---imported',
        isDir: true,
        path: '/N/scratch/cmguser/cmg-bioloop/imports/bigWig_h3k4me3_hg19---imported',
      }]);
      return;
    }

    if (!query_path) {
      logger.info('[FS] No query_path provided, returning empty array');
      res.json([]);
      return;
    }

    // if (process.env.NODE_ENV === 'docker') {
    //   const files = _.range(10).map((i) => ({
    //     name: `test${i}`,
    //     isDir: i % 2 === 0,
    //     path: path.join(query_path, `test${i}`),
    //   }));
    //   res.json(files);
    //   return;
    // }

    const base_dir_key = getBaseDirKey(req);
    const base_dir = getBaseDir(req);
    const mount_dir = get_mount_dir(req);
    const mounted_search_dir = get_mounted_search_dir(req);

    logger.info('[FS] Path resolution', {
      base_dir_key,
      base_dir,
      mount_dir,
      mounted_search_dir,
      query_path,
    });

    const { hasTrailingSlash } = req;

    fs.access(mounted_search_dir, constants.F_OK, (err) => {
      if (err) {
        logger.info('[FS] Exact path not found, attempting substring match', {
          mounted_search_dir,
          error: err.message,
          code: err.code,
        });

        const parent_query_path = path.dirname(query_path);
        const search_term = path.basename(query_path);

        const parent_mounted_dir = path.join(
          mount_dir,
          parent_query_path.slice(parent_query_path.indexOf(base_dir) + base_dir.length),
        );

        logger.info('[FS] Attempting case-insensitive substring match', {
          parent_query_path,
          search_term,
          parent_mounted_dir,
        });

        if (!parent_query_path.startsWith(base_dir)) {
          logger.warn('[FS] Parent path outside base_dir', {
            parent_query_path,
            base_dir,
          });
          res.json([]);
          return;
        }

        fs.access(parent_mounted_dir, constants.F_OK, (parentErr) => {
          if (parentErr) {
            logger.warn('[FS] Parent directory access failed', {
              parent_mounted_dir,
              error: parentErr.message,
            });
            res.json([]);
            return;
          }

          fs.readdir(parent_mounted_dir, { withFileTypes: true }, (readErr, files) => {
            if (readErr) {
              logger.error('[FS] Error reading parent directory', {
                parent_mounted_dir,
                error: readErr.message,
              });
              res.json([]);
              return;
            }

            let matchingFiles = files
              .filter((f) => {
                const nameMatches = f.name.toLowerCase().includes(search_term.toLowerCase());
                const isDirCheck = dirs_only ? f.isDirectory() : true;
                return nameMatches && isDirCheck;
              })
              .map((f) => ({
                name: f.name,
                isDir: f.isDirectory(),
                path: path.join(parent_query_path, f.name),
              }));

            // Filter by extension if provided
            if (extension && dirs_only) {
              const extensionFilterPromises = matchingFiles.map(async (file) => {
                if (!file.isDir) {
                  return file;
                }
                const mountedPath = path.join(
                  mount_dir,
                  file.path.slice(file.path.indexOf(base_dir) + base_dir.length),
                );
                const hasExtension = await directoryContainsExtension(mountedPath, extension);
                return hasExtension ? file : null;
              });

              Promise.all(extensionFilterPromises).then((filtered) => {
                matchingFiles = _.compact(filtered);

                logger.info('[FS] Substring match results (after extension filter)', {
                  search_term,
                  extension,
                  total_matches: matchingFiles.length,
                  matches: matchingFiles,
                });

                res.json(matchingFiles);
              });
            } else {
              logger.info('[FS] Substring match results', {
                search_term,
                total_matches: matchingFiles.length,
                matches: matchingFiles,
              });

              res.json(matchingFiles);
            }
          });
        });
        return;
      }

      if (!hasTrailingSlash) {
        logger.info('[FS] Exact path found without trailing slash, returning directory as match', {
          query_path,
        });

        const parent_query_path = path.dirname(query_path);
        const dir_name = path.basename(query_path);

        const dirResult = {
          name: dir_name,
          isDir: true,
          path: query_path,
        };

        // Filter by extension if provided
        if (extension && dirs_only) {
          directoryContainsExtension(mounted_search_dir, extension).then((hasExtension) => {
            if (hasExtension) {
              res.json([dirResult]);
            } else {
              res.json([]);
            }
          });
        } else {
          res.json([dirResult]);
        }
        return;
      }

      logger.info('[FS] Exact path found with trailing slash, returning directory contents', {
        mounted_search_dir,
      });

      fs.readdir(mounted_search_dir, {
        withFileTypes: true,
      }, (_err, files) => {
        if (_err) {
          logger.error('[FS] Error reading directory', {
            mounted_search_dir,
            error: _err.message,
            code: _err.code,
          });
          return next(createError.InternalServerError('Error reading directory'));
        }

        logger.info('[FS] Directory read successful', {
          mounted_search_dir,
          total_entries: files ? files.length : 0,
        });

        const allFilesData = files.map((f) => ({
          name: f.name,
          isDir: f.isDirectory(),
        }));

        logger.info('[FS] All entries in directory', {
          mounted_search_dir,
          entries: allFilesData,
        });

        let filesData = files.map((f) => {
          const file = {
            name: f.name,
            isDir: f.isDirectory(),
            path: path.join(query_path, f.name),
          };
          if (dirs_only) {
            return file.isDir ? file : null;
          }
          return file;
        });
        filesData = _.compact(filesData);

        // Filter by extension if provided
        if (extension && dirs_only) {
          const extensionFilterPromises = filesData.map(async (file) => {
            if (!file.isDir) {
              return file;
            }
            const mountedPath = path.join(mounted_search_dir, file.name);
            const hasExtension = await directoryContainsExtension(mountedPath, extension);
            return hasExtension ? file : null;
          });

          Promise.all(extensionFilterPromises).then((filtered) => {
            filesData = _.compact(filtered);

            logger.info('[FS] Response prepared (after extension filter)', {
              dirs_only,
              extension,
              total_before_filter: files ? files.length : 0,
              total_after_filter: filesData.length,
              result: filesData,
            });

            res.json(filesData);
          });
        } else {
          logger.info('[FS] Response prepared', {
            dirs_only,
            total_before_filter: files ? files.length : 0,
            total_after_filter: filesData.length,
            result: filesData,
          });

          res.json(filesData);
        }
      });
    });
  }),
);

// router.get(
//   '/dir-size',
//   validatePath,
//   asyncHandler(async (req, res) => {
//     const mounted_search_dir = get_mounted_search_dir(req);
//     console.log('mounted_search_dir: ', mounted_search_dir);
//
//     // check if the path is a directory
//     const stats = await fsPromises.stat(mounted_search_dir);
//     if (!stats.isDirectory()) {
//       console.log(mounted_search_dir, 'is not a directory');
//       res.status(400).send('Not a directory');
//     }
//
//     // As du -sb /path is a long running command,
//     // we will use SSE to keep connection alive and send the size when it's
//     // ready
//     res.setHeader('Content-Type', 'text/event-stream');
//     res.setHeader('Cache-Control', 'no-cache');
//     res.setHeader('Connection', 'keep-alive');
//     res.flushHeaders(); // flush the headers to establish SSE with client
//
//     console.log('sse started');
//
//     // get the size of the directory by spawning a child process to run "du -sb
//     // /path"
//     exec(`du -s ${mounted_search_dir}`, (err, stdout) => {
//       if (err) {
//         console.error('du -s');
//         console.error(err);
//         res.status(500).end();
//         return;
//       }
//       const size = parseInt(stdout.split('\t')[0], 10);
//       // send "message" type event to the client
//       console.log('before write');
//       res.write(`data: ${JSON.stringify({ size })}\n\n`);
//       res.write('event: done\ndata: \n\n');
//       console.log('before write');
//
//       res.end();
//     });
//   }),
// );

module.exports = router;

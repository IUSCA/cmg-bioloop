const express = require('express');
const { body, query, param } = require('express-validator');
const config = require('config');
const logger = require('@/services/logger');
const prisma = require('@/db');
const asyncHandler = require('../middleware/asyncHandler');
const { accessControl } = require('../middleware/auth');
const datasetService = require('../services/dataset');
const authService = require('../services/auth');

const router = express.Router();

// Middleware to check permissions
const isPermittedTo = accessControl('sessions');

// Helper function to get analysis type from dataset metadata
const getAnalysisType = (dataset) => dataset?.metadata?.analysis_type || null;

/**
 * Compute file's relative path for secure download
 * @param {Object} dataset - Dataset object with metadata.stage_alias
 * @param {Object} datasetFile - Dataset file object with path
 * @returns {string} Relative path for secure download (e.g., "/staged_data/datasets/42/sample.bam")
 */
function getRelativeFilePathForGenomeBrowser({ dataset, datasetFile }) {
  const stageAlias = dataset.metadata?.stage_alias || '';
  const filePath = datasetFile.path || '';

  const cleanedStageAlias = stageAlias.replace(/^\/+/, '');
  const cleanedFilePath = filePath.replace(/^\/+/, '');

  return `/${cleanedStageAlias}/${cleanedFilePath}`;
}

/**
 * Get file-scoped token for genome browser access
 * @param {string} relativePath - Relative path to the file
 * @returns {Promise<string>} JWT token
 */
async function getGenomeBrowserFileToken(relativePath) {
  // Use the same OAuth2 pattern as regular downloads but with file exposure scope
  // The authService.get_file_exposure_token() uses the genome_browser OAuth2 client
  const tokenResponse = await authService.get_file_exposure_token(relativePath);
  return tokenResponse.accessToken;
}

/**
 * Build secure download URL for genome browser
 * @param {string} relativePath - Relative path to the file
 * @param {string} token - JWT token
 * @returns {string} Complete URL for secure download
 */
function buildGenomeBrowserUrl(relativePath, token) {
  const baseUrl = config.get('secure_download.base_url'); // "http://localhost:3060"
  const prefix = config.get('secure_download.genome_browser_path_prefix'); // "/files/expose"

  const url = new URL(baseUrl);
  url.pathname = `${prefix}${relativePath}`;
  url.searchParams.set('token', token);

  return url.toString();
}

/**
 * Determines WashU browser file type from file path/name
 */
const getWashUFileType = (filePath) => {
  if (!filePath) return 'unknown';
  const lowerPath = filePath.toLowerCase();

  // Map extensions to WashU types
  if (lowerPath.endsWith('.bam')) return 'bam';
  if (lowerPath.endsWith('.bw') || lowerPath.endsWith('.bigwig')) return 'bigwig';
  if (lowerPath.endsWith('.vcf')) return 'vcf';
  return 'unknown';
};

// GET /sessions - Get all sessions accessible to the current user
router.get(
  '/',
  isPermittedTo('read'),
  [
    query('title').trim().optional(),
    query('genome').trim().optional(),
    query('genome_type').trim().optional(),
    query('limit').isInt({ min: 1, max: 100 }).optional().toInt(),
    query('offset').isInt({ min: 0 }).optional().toInt(),
    query('sort_by').isIn(['title', 'genome', 'created_at', 'updated_at']).optional(),
    query('sort_order').isIn(['asc', 'desc']).optional(),
  ],
  asyncHandler(async (req, res) => {
    const {
      title,
      genome,
      genome_type,
      limit = 25,
      offset = 0,
      sort_by = 'created_at',
      sort_order = 'desc',
    } = req.query;

    // Build filter query - admin/operator can see all sessions
    let filter_query;
    if (req.permission.granted) {
      // Admin/operator can see all sessions
      filter_query = {};
    } else {
      // Regular users can only see their own sessions and public ones
      filter_query = {
        OR: [
          { user_id: req.user.id }, // User's own sessions
          { is_public: true }, // Public sessions
        ],
      };
    }

    if (title) {
      filter_query.title = { contains: title, mode: 'insensitive' };
    }
    if (genome) {
      filter_query.genome = { contains: genome, mode: 'insensitive' };
    }
    if (genome_type) {
      filter_query.genome_type = { contains: genome_type, mode: 'insensitive' };
    }

    // Get sessions with related data
    const [sessions, total] = await Promise.all([
      prisma.genome_browser_session.findMany({
        where: filter_query,
        include: {
          user: {
            select: {
              id: true,
              username: true,
              name: true,
            },
          },
          session_tracks: {
            include: {
              track: {
                include: {
                  dataset_file: {
                    include: {
                      dataset: true,
                    },
                  },
                },
              },
            },
            orderBy: { order: 'asc' },
          },
          _count: {
            select: {
              session_tracks: true,
            },
          },
        },
        orderBy: { [sort_by]: sort_order },
        take: limit,
        skip: offset,
      }),
      prisma.genome_browser_session.count({ where: filter_query }),
    ]);

    res.json({
      sessions,
      metadata: {
        count: total,
      },
    });
  }),
);

// GET /sessions/:username - Get sessions for a specific user (if accessible)
router.get(
  'all/:username',
  isPermittedTo('read'),
  [
    param('username').isString().trim(),
    query('title').trim().optional(),
    query('genome').trim().optional(),
    query('genome_type').trim().optional(),
    query('limit').isInt({ min: 1, max: 100 }).optional().toInt(),
    query('offset').isInt({ min: 0 }).optional().toInt(),
    query('sort_by').isIn(['title', 'genome', 'created_at', 'updated_at']).optional(),
    query('sort_order').isIn(['asc', 'desc']).optional(),
  ],
  asyncHandler(async (req, res) => {
    const { username } = req.params;
    const {
      title,
      genome,
      genome_type,
      limit = 25,
      offset = 0,
      sort_by = 'created_at',
      sort_order = 'desc',
    } = req.query;

    // Find the user
    const targetUser = await prisma.user.findUnique({
      where: { username },
    });

    if (!targetUser) {
      return res.status(404).json({ error: 'User not found' });
    }

    // Build filter query - admin/operator can see all sessions
    let filter_query;
    if (req.permission.granted) {
      // Admin/operator can see all sessions for any user
      filter_query = {
        user_id: targetUser.id,
      };
    } else {
      // Regular users can only see their own sessions and public ones
      filter_query = {
        user_id: targetUser.id,
        OR: [
          { user_id: req.user.id }, // User's own sessions
          { is_public: true }, // Public sessions
        ],
      };
    }

    if (title) {
      filter_query.title = { contains: title, mode: 'insensitive' };
    }
    if (genome) {
      filter_query.genome = { contains: genome, mode: 'insensitive' };
    }
    if (genome_type) {
      filter_query.genome_type = { contains: genome_type, mode: 'insensitive' };
    }

    // Get sessions with related data
    const [sessions, total] = await Promise.all([
      prisma.genome_browser_session.findMany({
        where: filter_query,
        include: {
          user: {
            select: {
              id: true,
              username: true,
              name: true,
            },
          },
          session_tracks: {
            include: {
              track: {
                include: {
                  dataset_file: {
                    include: {
                      dataset: true,
                    },
                  },
                },
              },
            },
            orderBy: { order: 'asc' },
          },
          _count: {
            select: {
              session_tracks: true,
            },
          },
        },
        orderBy: { [sort_by]: sort_order },
        take: limit,
        skip: offset,
      }),
      prisma.genome_browser_session.count({ where: filter_query }),
    ]);

    res.json({
      sessions,
      metadata: {
        count: total,
      },
    });
  }),
);

// GET /sessions/check-name/:name - Check if session name exists for current user
router.get(
  '/check-name/:name',
  isPermittedTo('create'),
  [param('name').isString().notEmpty().trim()],
  asyncHandler(async (req, res) => {
    const { name } = req.params;

    const existingSession = await prisma.genome_browser_session.findFirst({
      where: {
        title: name,
        user_id: req.user.id,
      },
    });

    res.json({ exists: !!existingSession });
  }),
);

// POST /sessions - Create a new session
// Accepts track_ids directly (no auto-creation of tracks)
router.post(
  '/',
  isPermittedTo('create'),
  [
    body('session_name').isString().notEmpty().trim(),
    body('genome').isString().optional().trim(),
    body('genome_type').isString().optional().trim(),
    body('track_ids').isArray().notEmpty(),
    body('track_ids').custom((value) => {
      if (!Array.isArray(value)) {
        throw new Error('track_ids must be an array');
      }
      if (value.some((id) => !Number.isInteger(id))) {
        throw new Error('All track_ids must be integers');
      }
      return true;
    }),
    body('is_public').isBoolean().optional(),
  ],
  asyncHandler(async (req, res) => {
    const {
      session_name,
      genome: providedGenome,
      genome_type: providedGenomeType,
      track_ids,
      is_public = false,
    } = req.body;

    // Fetch the specified tracks
    const tracks = await prisma.track.findMany({
      where: {
        id: { in: track_ids },
      },
      include: {
        dataset_file: {
          include: {
            dataset: {
              include: {
                genomic_details: true,
              },
            },
          },
        },
      },
    });

    if (tracks.length !== track_ids.length) {
      return res.status(400).json({ error: 'Some tracks are not accessible or not found' });
    }

    // Validate tracks for session compatibility
    const validationResult = validateTracksForSession(
      tracks,
      providedGenomeType,
      providedGenome,
    );
    if (!validationResult.isValid) {
      return res.status(400).json({ error: validationResult.error });
    }

    // Use provided genome values or leave empty (no auto-derivation)
    const finalGenomeType = providedGenomeType || null;
    const finalGenome = providedGenome || null;

    // Create session with tracks
    const session = await prisma.genome_browser_session.create({
      data: {
        title: session_name,
        genome: finalGenome,
        genome_type: finalGenomeType,
        user_id: req.user.id,
        is_public,
        session_tracks: {
          create: tracks.map((track, index) => ({
            track_id: track.id,
            order: index,
          })),
        },
      },
      include: {
        user: {
          select: {
            id: true,
            username: true,
            name: true,
          },
        },
        session_tracks: {
          include: {
            track: {
              include: {
                dataset_file: {
                  include: {
                    dataset: {
                      include: {
                        genomic_details: true,
                      },
                    },
                  },
                },
              },
            },
          },
          orderBy: { order: 'asc' },
        },
      },
    });

    res.status(201).json(session);
  }),
);

// GET /sessions/:id - Get a specific session
router.get(
  '/:id',
  isPermittedTo('read'),
  [param('id').isInt().toInt()],
  asyncHandler(async (req, res) => {
    const { id } = req.params;

    logger.info(`GET /sessions/${id}`);

    const session = await prisma.genome_browser_session.findUnique({
      where: { id },
      include: {
        user: {
          select: {
            id: true,
            username: true,
            name: true,
          },
        },
        session_tracks: {
          include: {
            track: {
              include: {
                dataset_file: {
                  include: {
                    dataset: true,
                  },
                },
              },
            },
          },
          orderBy: { order: 'asc' },
        },
        _count: {
          select: {
            session_tracks: true,
          },
        },
      },
    });

    if (!session) {
      return res.status(404).json({ error: 'Session not found' });
    }

    // If user has admin/operator role, they can see any session
    // Otherwise, check access permissions
    const canAccess = req.permission.granted
      || session.user_id === req.user.id
      || session.is_public;

    if (!canAccess) {
      return res.status(403).json({ error: 'Access denied' });
    }

    // Increment access count
    await prisma.genome_browser_session.update({
      where: { id },
      data: { access_count: { increment: 1 } },
    });

    res.json(session);
  }),
);

// PATCH /sessions/:id - Update a session
router.patch(
  '/:id',
  isPermittedTo('update'),
  [
    param('id').isInt().toInt(),
    body('title').isString().optional().trim(),
    body('genome').isString().optional().trim(),
    body('genome_type').isString().optional().trim(),
    body('is_public').isBoolean().optional(),
    body('track_ids').isArray().optional(),
    body('track_ids').custom((value) => {
      if (value && !Array.isArray(value)) {
        throw new Error('track_ids must be an array');
      }
      if (value && value.some((id) => !Number.isInteger(id))) {
        throw new Error('All track_ids must be integers');
      }
      return true;
    }),
  ],
  asyncHandler(async (req, res) => {
    const { id } = req.params;
    const {
      title, genome, genome_type, is_public, track_ids,
    } = req.body;

    // Check if session exists and user has access
    const existingSession = await prisma.genome_browser_session.findUnique({
      where: { id },
      include: {
        user: true,
      },
    });

    if (!existingSession) {
      return res.status(404).json({ error: 'Session not found' });
    }

    if (existingSession.user_id !== req.user.id) {
      return res.status(403).json({ error: 'Access denied' });
    }

    // Validate tracks if provided
    if (track_ids && track_ids.length > 0) {
      let tracks;

      // If user has admin/operator role, they can access all tracks
      if (req.permission.granted) {
        tracks = await prisma.track.findMany({
          where: {
            id: { in: track_ids },
          },
        });
      } else {
        // Regular users can only access tracks from projects they're part of
        tracks = await prisma.track.findMany({
          where: {
            id: { in: track_ids },
            dataset_file: {
              dataset: {
                projects: {
                  some: {
                    project: {
                      users: {
                        some: { user_id: req.user.id },
                      },
                    },
                  },
                },
              },
            },
          },
        });
      }

      if (tracks.length !== track_ids.length) {
        return res.status(400).json({ error: 'Some tracks are not accessible' });
      }

      // Validate tracks for session compatibility
      const validationResult = validateTracksForSession(tracks, genome_type, genome);
      if (!validationResult.isValid) {
        return res.status(400).json({ error: validationResult.error });
      }
    }

    // Update session
    const updateData = {};
    if (title !== undefined) updateData.title = title;
    if (genome !== undefined) updateData.genome = genome;
    if (genome_type !== undefined) updateData.genome_type = genome_type;
    if (is_public !== undefined) updateData.is_public = is_public;

    const session = await prisma.genome_browser_session.update({
      where: { id },
      data: {
        ...updateData,
        ...(track_ids && {
          session_tracks: {
            deleteMany: {},
            create: track_ids.map((track_id, index) => ({
              track_id,
              order: index,
            })),
          },
        }),
      },
      include: {
        user: {
          select: {
            id: true,
            username: true,
            name: true,
          },
        },
        session_tracks: {
          include: {
            track: {
              include: {
                dataset_file: {
                  include: {
                    dataset: true,
                  },
                },
              },
            },
          },
          orderBy: { order: 'asc' },
        },
        _count: {
          select: {
            session_tracks: true,
          },
        },
      },
    });

    res.json(session);
  }),
);

// DELETE /sessions/:id - Delete a session
router.delete(
  '/:id',
  [param('id').isInt().toInt()],
  asyncHandler(async (req, res) => {
    const { id } = req.params;

    // Check if session exists and user has access
    const existingSession = await prisma.genome_browser_session.findUnique({
      where: { id },
      include: {
        user: true,
      },
    });

    if (!existingSession) {
      return res.status(404).json({ error: 'Session not found' });
    }

    if (existingSession.user_id !== req.user.id) {
      return res.status(403).json({ error: 'Access denied' });
    }

    // Delete session (cascade will handle related records)
    await prisma.genome_browser_session.delete({
      where: { id },
    });

    res.status(204).send();
  }),
);

// GET /sessions/:id/datahub - Export session tracks in WashU browser format
// This endpoint is only accessible to admin/operator roles
router.get(
  '/:id/datahub',
  isPermittedTo('read'), // Only admin/operator can access WashU browser integration
  [param('id').isInt().toInt()],
  asyncHandler(async (req, res) => {
    const sessionId = req.params.id;

    try {
      const session = await prisma.genome_browser_session.findUnique({
        where: { id: sessionId },
        include: {
          session_tracks: {
            include: {
              track: {
                include: {
                  dataset_file: {
                    include: {
                      dataset: {
                        include: {
                          genomic_details: true,
                        },
                      },
                    },
                  },
                },
              },
            },
            orderBy: { order: 'asc' },
          },
        },
      });

      if (!session) {
        return res.status(404).json({ error: 'Session not found' });
      }

      // Convert to WashU DataHub format using secure_download
      // Filter tracks to only include browser-compatible files
      const tracks = await Promise.all(
        session.session_tracks
          .map(async (st) => {
            const { track } = st;
            const { dataset_file: datasetFile } = track;
            const { dataset } = datasetFile;
            const filePath = datasetFile?.path || datasetFile?.name || '';

            // Determine file type from extension
            const fileType = getWashUFileType(filePath);

            // Get genome info from dataset (since genome is associated with dataset in bioloop)
            const genomeInfo = dataset?.genomic_details;
            const genomeType = genomeInfo?.genome_type || session.genome_type || '';
            const genomeValue = genomeInfo?.genome_value || session.genome || '';

            // Generate secure download URL for WashU browser
            try {
              const relativePath = getRelativeFilePathForGenomeBrowser({ dataset, datasetFile });
              const token = await getGenomeBrowserFileToken(relativePath);
              const url = buildGenomeBrowserUrl(relativePath, token);

              // Track display name (use session track title if available, else track name)
              const trackName = st.title || track.name || datasetFile.name || 'Unnamed Track';

              return {
                type: fileType,
                name: trackName,
                options: {
                  color: st.color || '#2669a3',
                  height: 100,
                },
                showOnHubLoad: true,
                url,
                // Include genome info if available (for reference, WashU uses genome from URL params)
                ...(genomeType && genomeValue ? { genome: `${genomeType}_${genomeValue}` } : {}),
              };
            } catch (error) {
              logger.error(`Failed to generate secure URL for file ${datasetFile.id}:`, error);
              return null; // Skip this track if URL generation fails
            }
          }),
      );

      // Filter out any null tracks (failed URL generation)
      const validTracks = tracks.filter(Boolean);

      res.json(validTracks);
    } catch (error) {
      logger.error('Error exporting DataHub:', error);
      res.status(500).json({ error: 'Failed to export DataHub' });
    }
  }),
);

// POST /sessions/:id/stage
router.post(
  '/:id/stage',
  [param('id').isInt().toInt()],
  asyncHandler(async (req, res) => {
    const sessionId = req.params.id;

    try {
      const session = await prisma.genome_browser_session.findUnique({
        where: { id: sessionId },
        include: {
          session_tracks: {
            include: {
              track: {
                include: {
                  dataset_file: {
                    include: {
                      dataset: true,
                    },
                  },
                },
              },
            },
          },
        },
      });

      if (!session) {
        return res.status(404).json({ error: 'Session not found' });
      }

      // Get unique datasets that need staging
      const datasetsToStage = [...new Set(
        session.session_tracks
          .filter((st) => !st.track.dataset_file?.dataset?.is_staged)
          .map((st) => st.track.dataset_file?.dataset?.id)
          .filter(Boolean),
      )];

      if (datasetsToStage.length === 0) {
        return res.json({ message: 'All datasets are already staged' });
      }

      // Return the datasets that need staging so the frontend can call the staging workflow
      res.json({
        message: 'Datasets need staging',
        datasets: datasetsToStage,
        note: 'Use the dataset staging workflow to stage these datasets individually',
      });
    } catch (error) {
      console.error('Error checking staging status:', error);
      res.status(500).json({ error: 'Failed to check staging status' });
    }
  }),
);

// GET /sessions/:id/projects - Get projects associated with a session
router.get(
  '/:id/projects',
  isPermittedTo('read'),
  [
    param('id').isInt().toInt(),
    query('limit').isInt({ min: 1, max: 100 }).optional().toInt(),
    query('offset').isInt({ min: 0 }).optional().toInt(),
    query('sort_by').isIn(['name', 'created_at', 'updated_at']).optional(),
    query('sort_order').isIn(['asc', 'desc']).optional(),
    query('search').trim().optional(),
  ],
  asyncHandler(async (req, res) => {
    const { id } = req.params;
    const {
      limit = 25,
      offset = 0,
      sort_by = 'name',
      sort_order = 'asc',
      search,
    } = req.query;

    try {
      // First, get the session to verify it exists and user has access
      let sessionWhere;
      if (req.permission.granted) {
        // Admin/operator can see any session
        sessionWhere = { id };
      } else {
        // Regular users can only see their own sessions and public ones
        sessionWhere = {
          id,
          OR: [
            { user_id: req.user.id }, // User's own sessions
            { is_public: true }, // Public sessions
          ],
        };
      }

      const session = await prisma.genome_browser_session.findFirst({
        where: sessionWhere,
        include: {
          session_tracks: {
            include: {
              track: {
                include: {
                  dataset_file: {
                    include: {
                      dataset: {
                        include: {
                          projects: {
                            include: {
                              project: true,
                            },
                          },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      });

      if (!session) {
        return res.status(404).json({ error: 'Session not found or access denied' });
      }

      // Extract unique projects from session tracks
      const projectMap = new Map();

      session.session_tracks.forEach((sessionTrack) => {
        const dataset = sessionTrack.track.dataset_file?.dataset;
        if (dataset?.projects) {
          dataset.projects.forEach((projectAssoc) => {
            const { project } = projectAssoc;
            if (!projectMap.has(project.id)) {
              projectMap.set(project.id, {
                id: project.id,
                name: project.name,
                slug: project.slug,
                description: project.description,
                created_at: project.created_at,
                updated_at: project.updated_at,
              });
            }
          });
        }
      });

      let projects = Array.from(projectMap.values());

      // Apply search filter
      if (search) {
        const searchLower = search.toLowerCase();
        projects = projects.filter((project) => {
          const nameMatch = project.name.toLowerCase().includes(searchLower);
          const descMatch = project.description
            && project.description.toLowerCase().includes(searchLower);
          return nameMatch || descMatch;
        });
      }

      // Apply sorting
      projects.sort((a, b) => {
        const aVal = a[sort_by];
        const bVal = b[sort_by];

        if (sort_order === 'asc') {
          if (aVal < bVal) return -1;
          if (aVal > bVal) return 1;
          return 0;
        }
        if (aVal > bVal) return -1;
        if (aVal < bVal) return 1;
        return 0;
      });

      // Apply pagination
      const total = projects.length;
      const paginatedProjects = projects.slice(offset, offset + limit);

      res.json({
        projects: paginatedProjects,
        metadata: {
          count: total,
          limit,
          offset,
          sort_by,
          sort_order,
        },
      });
    } catch (error) {
      console.error('Error fetching session projects:', error);
      res.status(500).json({ error: 'Failed to fetch session projects' });
    }
  }),
);

// Track validation functions
const validateTracksForSession = (tracks, sessionGenomeType, sessionGenome) => {
  if (!tracks || tracks.length === 0) {
    return { isValid: false, error: 'No tracks provided for validation' };
  }

  const firstTrack = tracks[0];
  const firstGenomeDetails = firstTrack?.dataset_file?.dataset?.genomic_details;

  // Validation 1: Check genome type consistency (MANDATORY)
  const inconsistentGenomeType = tracks.find((track) => {
    const genomeDetails = track?.dataset_file?.dataset?.genomic_details;
    return genomeDetails?.genome_type !== firstGenomeDetails?.genome_type;
  });
  if (inconsistentGenomeType) {
    const inconsistentGenomeDetails = inconsistentGenomeType?.dataset_file?.dataset?.genomic_details;
    return {
      isValid: false,
      error: `Cannot mix different genome types. Track "${firstTrack.name}" `
        + `has "${firstGenomeDetails?.genome_type || 'unknown'}", track "${inconsistentGenomeType.name}" `
        + `has "${inconsistentGenomeDetails?.genome_type || 'unknown'}". Please create separate sessions.`,
    };
  }

  // Validation 2: Check genome value consistency
  const inconsistentGenomeValue = tracks.find((track) => {
    const genomeDetails = track?.dataset_file?.dataset?.genomic_details;
    return genomeDetails?.genome_value !== firstGenomeDetails?.genome_value;
  });
  if (inconsistentGenomeValue) {
    const inconsistentGenomeDetails = inconsistentGenomeValue?.dataset_file?.dataset?.genomic_details;
    return {
      isValid: false,
      error: `Cannot mix different genome assemblies. Track "${firstTrack.name}" `
        + `has "${firstGenomeDetails?.genome_value || 'unknown'}", track "${inconsistentGenomeValue.name}" `
        + `has "${inconsistentGenomeDetails?.genome_value || 'unknown'}". `
        + 'Please create separate sessions or manually select one assembly.',
    };
  }

  // Validation 3: Check if session genome matches track genomes
  if (sessionGenomeType && sessionGenomeType !== firstGenomeDetails?.genome_type) {
    return {
      isValid: false,
      error: `Session genome type "${sessionGenomeType}" `
        + `does not match track genome type "${firstGenomeDetails?.genome_type || 'unknown'}"`,
    };
  }

  if (sessionGenome && sessionGenome !== firstGenomeDetails?.genome_value) {
    return {
      isValid: false,
      error: `Session genome assembly "${sessionGenome}" `
        + `does not match track genome assembly "${firstGenomeDetails?.genome_value || 'unknown'}"`,
    };
  }

  return { isValid: true };
};

router.get(
  '/:id/tracks',
  [param('id').isInt().toInt()],
  asyncHandler(async (req, res) => {
    const sessionId = req.params.id;
    const session = await prisma.genome_browser_session.findUnique({
      where: { id: sessionId },
      include: {
        session_tracks: {
          include: {
            track: {
              include: {
                dataset_file: {
                  include: {
                    dataset: true,
                  },
                },
              },
            },
          },
        },
      },
    });

    const tracks = session.session_tracks.map((st) => {
      const { track } = st;
      const track_file = track.dataset_file;
      const dataset = track_file?.dataset;
      return {
        name: track.name,
        type: getAnalysisType(dataset) || 'unknown',
        options: {
          color: st.color,
          height: 100,
        },
        showOnHubLoad: true,
        url: datasetService.get_download_url({
          dataset, file: track_file,
        }),
      };
    });

    res.json(tracks);
  }),
);

// NOTE: The old /sessions/:session_id/files/:file_id endpoint has been removed
// Files are now served securely via the secure_download microservice at /genome-browser/*
// with proper token-based authorization

module.exports = router;

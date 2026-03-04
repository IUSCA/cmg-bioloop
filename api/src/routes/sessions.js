const express = require('express');
const { body, query, param } = require('express-validator');
const config = require('config');
const fs = require('fs');
const path = require('path');
const createError = require('http-errors');
const CONSTANTS = require('@/constants');
const { DONE_STATUSES, DATA_REQUEST_STATUS, WORKFLOWS } = CONSTANTS;
const prisma = require('@/db');
const logger = require('@/services/logger');
const pathResolver = require('@/services/pathResolver');
const wfService = require('@/services/workflow');
const legacyMigrationService = require('@/services/legacyMigration');
const { validate } = require('@/middleware/validators');
const asyncHandler = require('../middleware/asyncHandler');
const { accessControl, authenticateWithCookie } = require('../middleware/auth');
const datasetService = require('../services/dataset');
const { findIndexFileForPrimary } = require('../utils/genomeBrowserUtils');

const router = express.Router();
const fileExposureRouter = express.Router();

// Genome Browser Constants
const BROWSER_TYPES = {
  IGV: 'igv',
  WASHU: 'washu',
};

// Middleware to check permissions
const isPermittedTo = accessControl('sessions');

/**
 * Returns true if the authenticated user has admin or operator role.
 * Used to distinguish privileged access (all data) from user-level access (own data).
 */
const userCanAccessAll = (user) => (user?.roles || []).some((r) => ['admin', 'operator'].includes(r));

const sessionOwnerFn = async (req) => {
  const session = await prisma.genome_browser_session.findUnique({
    where: { id: parseInt(req.params.id) },
    select: { user: { select: { username: true } } },
  });
  return session?.user?.username;
};

// Helper function to get analysis type from dataset metadata
const getAnalysisType = (dataset) => dataset?.metadata?.analysis_type || null;

/**
 * Evaluate data request status for a session based on workflow states
 * @param {Object} session - Session object with session_tracks populated
 * @param {Array} enrichedWorkflowsByDataset - Optional map of dataset ID to enriched workflows from Rhythm
 * @returns {Object} { requested: boolean, all_staged: boolean, request_status?: 'PENDING' | 'COMPLETE' }
 */
function evaluateDataRequestStatus(session, enrichedWorkflowsByDataset = {}) {
  if (!session?.session_tracks || session.session_tracks.length === 0) {
    return { requested: false, all_staged: true };
  }

  // Get unique datasets from session tracks
  const datasetMap = new Map();
  session.session_tracks.forEach((st) => {
    const dataset = st.track?.dataset_file?.dataset;
    if (dataset) {
      datasetMap.set(dataset.id, dataset);
    }
  });

  const datasets = Array.from(datasetMap.values());

  // Check if any datasets are unstaged
  const unstagedDatasets = datasets.filter((ds) => !ds.is_staged);

  if (unstagedDatasets.length === 0) {
    // All datasets are staged - no request needed
    return { requested: false, all_staged: true };
  }

  // Check workflow status for unstaged datasets
  let hasPendingWorkflows = false;
  let allHaveWorkflows = true;

  unstagedDatasets.forEach((ds) => {
    // Use enriched workflows if provided, otherwise fall back to DB workflows (which only have id)
    const workflows = enrichedWorkflowsByDataset[ds.id] || ds.workflows || [];
    const stageWorkflows = workflows.filter((wf) => wf.name === 'stage');

    if (stageWorkflows.length === 0) {
      // No stage workflow exists for this dataset
      allHaveWorkflows = false;
    } else {
      // Check if any stage workflow is still pending/running
      const hasActiveWorkflow = stageWorkflows.some(
        (wf) => !DONE_STATUSES.includes(wf.status),
      );
      if (hasActiveWorkflow) {
        hasPendingWorkflows = true;
      }
    }
  });

  // If no workflows exist for unstaged datasets, data not requested
  if (!allHaveWorkflows && !hasPendingWorkflows) {
    return { requested: false, all_staged: false };
  }

  // Data has been requested
  return {
    requested: true,
    all_staged: false,
    request_status: hasPendingWorkflows || !allHaveWorkflows
      ? DATA_REQUEST_STATUS.PENDING
      : DATA_REQUEST_STATUS.COMPLETE,
  };
}

// Path resolution is now handled by pathResolver service
// See: api/src/services/pathResolver.js

/**
 * Build file exposure URL for genome browsers (session-scoped, relative)
 * @param {number} sessionId - Session ID
 * @param {string} relativePath - Relative path to the file
 * @returns {string} Relative API URL
 */
function buildFileExposureUrl(sessionId, relativePath) {
  const cleanedPath = relativePath.replace(/^\/+/, '');
  return `/api/sessions/${sessionId}/files/expose/${cleanedPath}`;
}

/**
 * Determines file type configuration for genome browsers (IGV and WashU)
 * Returns config that can be serialized for different browser types
 * @param {string} filePath - File path or name
 * @returns {Object|null} Browser-specific track configuration with igv and washu properties
 */
const getGenomeBrowserFileConfig = (filePath) => {
  if (!filePath) return null;
  const lowerPath = filePath.toLowerCase();

  // Map extensions to browser-specific configurations
  if (lowerPath.endsWith('.bam')) {
    return {
      baseType: 'alignment',
      extension: 'bam',
      igv: { type: 'alignment', format: 'bam' },
      washu: { type: 'bam' },
    };
  }

  if (lowerPath.endsWith('.bw') || lowerPath.endsWith('.bigwig')) {
    return {
      baseType: 'signal',
      extension: 'bigwig',
      igv: { type: 'wig', format: 'bigwig' },
      washu: { type: 'bigwig' },
    };
  }

  if (lowerPath.endsWith('.wig')) {
    return {
      baseType: 'signal',
      extension: 'wig',
      igv: { type: 'wig', format: 'wig' },
      washu: { type: 'bigwig' },
    };
  }

  if (lowerPath.endsWith('.vcf') || lowerPath.endsWith('.vcf.gz')) {
    return {
      baseType: 'variant',
      extension: 'vcf',
      igv: { type: 'variant', format: 'vcf' },
      washu: { type: 'vcf' },
    };
  }

  if (lowerPath.endsWith('.bed')) {
    return {
      baseType: 'annotation',
      extension: 'bed',
      igv: { type: 'annotation', format: 'bed' },
      washu: { type: 'bed' },
    };
  }

  if (lowerPath.endsWith('.gff') || lowerPath.endsWith('.gff3')) {
    return {
      baseType: 'annotation',
      extension: 'gff3',
      igv: { type: 'annotation', format: 'gff3' },
      washu: { type: 'gff' },
    };
  }

  if (lowerPath.endsWith('.gtf')) {
    return {
      baseType: 'annotation',
      extension: 'gtf',
      igv: { type: 'annotation', format: 'gtf' },
      washu: { type: 'gtf' },
    };
  }

  return null;
};

/**
 * Serialize track for IGV browser
 * @param {Object} sessionTrack - Session track object with track, dataset_file, dataset
 * @param {number} sessionId - Session ID for URL generation
 * @param {Object} filesByDataset - Map of dataset files for index lookup
 * @returns {Object|null} IGV-compatible track configuration
 */
function serializeTrackForIGV(sessionTrack, sessionId, filesByDataset) {
  const { track } = sessionTrack;
  const { dataset_file: datasetFile } = track;
  const { dataset } = datasetFile;
  const filePath = datasetFile?.path || datasetFile?.name || '';

  const fileConfig = getGenomeBrowserFileConfig(filePath);
  if (!fileConfig) {
    logger.warn(`[IGV] Unsupported file type: ${filePath}`);
    return null;
  }

  const relativePath = pathResolver.getRelativeFilePath({ dataset, datasetFile });
  const url = buildFileExposureUrl(sessionId, relativePath);
  const trackName = sessionTrack.title || track.name || datasetFile.name || 'Unnamed Track';

  const trackConfig = {
    type: fileConfig.igv.type,
    format: fileConfig.igv.format,
    name: trackName,
    url,
    color: sessionTrack.color || '#2669a3',
    height: 100,
  };

  // Attach index file if exists (for BAM/VCF)
  const datasetFilesForThisDataset = filesByDataset[dataset.id] || [];
  const indexFile = findIndexFileForPrimary(datasetFilesForThisDataset, datasetFile);

  if (indexFile) {
    // Use the index file's dataset object (same as primary file's dataset)
    const indexRelativePath = pathResolver.getRelativeFilePath({
      dataset: indexFile.dataset || dataset,
      datasetFile: indexFile,
    });
    const indexUrl = buildFileExposureUrl(sessionId, indexRelativePath);
    trackConfig.indexURL = indexUrl;
  }

  return trackConfig;
}

/**
 * Serialize track for WashU browser
 * @param {Object} sessionTrack - Session track object with track, dataset_file, dataset
 * @param {number} sessionId - Session ID for URL generation
 * @param {Object} filesByDataset - Map of dataset files for index lookup
 * @returns {Object|null} WashU-compatible track configuration
 */
function serializeTrackForWashU(sessionTrack, sessionId, filesByDataset) {
  const { track } = sessionTrack;
  const { dataset_file: datasetFile } = track;
  const { dataset } = datasetFile;
  const filePath = datasetFile?.path || datasetFile?.name || '';

  const fileConfig = getGenomeBrowserFileConfig(filePath);
  if (!fileConfig) {
    logger.warn(`[WashU] Unsupported file type: ${filePath}`);
    return null;
  }

  const relativePath = pathResolver.getRelativeFilePath({ dataset, datasetFile });
  const url = buildFileExposureUrl(sessionId, relativePath);
  const trackName = sessionTrack.title || track.name || datasetFile.name || 'Unnamed Track';

  // WashU format per https://eg.readthedocs.io/en/latest/datahub.html
  const trackConfig = {
    type: fileConfig.washu.type,
    name: trackName,
    url,
    options: {
      color: sessionTrack.color || '#2669a3',
      height: 100,
    },
  };

  // Attach index file if exists (for BAM/VCF)
  const datasetFilesForThisDataset = filesByDataset[dataset.id] || [];
  const indexFile = findIndexFileForPrimary(datasetFilesForThisDataset, datasetFile);

  if (indexFile) {
    // Use the index file's dataset object (same as primary file's dataset)
    const indexRelativePath = pathResolver.getRelativeFilePath({
      dataset: indexFile.dataset || dataset,
      datasetFile: indexFile,
    });
    const indexUrl = buildFileExposureUrl(sessionId, indexRelativePath);
    trackConfig.indexURL = indexUrl;
  }

  return trackConfig;
}

// GET /sessions - Get all sessions accessible to the current user
router.get(
  '/',
  isPermittedTo('read'),
  [
    query('title').trim().optional(),
    query('genome').trim().optional(),
    query('genome_type').trim().optional(),
    query('dataset_id').optional().isInt({ min: 1 }).toInt(),
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
      dataset_id,
      limit = 25,
      offset = 0,
      sort_by = 'created_at',
      sort_order = 'desc',
    } = req.query;

    // Build filter query - admin/operator see all; user role sees only their own sessions
    let filter_query;
    if (userCanAccessAll(req.user)) {
      filter_query = {};
    } else {
      filter_query = { user_id: req.user.id };
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
    if (dataset_id) {
      filter_query.session_tracks = {
        some: {
          track: {
            dataset_file: {
              dataset_id,
            },
          },
        },
      };
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

// GET /sessions/:username/all - Get sessions for a specific user (if accessible)
router.get(
  '/:username/all',
  isPermittedTo('read', { checkOwnership: true }),
  [
    param('username').isString().trim(),
    query('title').trim().optional(),
    query('genome').trim().optional(),
    query('genome_type').trim().optional(),
    query('dataset_id').optional().isInt({ min: 1 }).toInt(),
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
      dataset_id,
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

    // Build filter query scoped to the target user
    const filter_query = { user_id: targetUser.id };

    if (title) {
      filter_query.title = { contains: title, mode: 'insensitive' };
    }
    if (genome) {
      filter_query.genome = { contains: genome, mode: 'insensitive' };
    }
    if (genome_type) {
      filter_query.genome_type = { contains: genome_type, mode: 'insensitive' };
    }
    if (dataset_id) {
      filter_query.session_tracks = {
        some: {
          track: {
            dataset_file: {
              dataset_id,
            },
          },
        },
      };
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

    // Fetch the specified tracks, filtering by project membership for non-privileged users
    const trackWhere = { id: { in: track_ids } };
    if (!userCanAccessAll(req.user)) {
      trackWhere.dataset_file = {
        dataset: {
          projects: {
            some: {
              project: {
                users: { some: { user_id: req.user.id } },
              },
            },
          },
        },
      };
    }

    const tracks = await prisma.track.findMany({
      where: trackWhere,
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
    const validationResult = validateTracksForSession(tracks);
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
  isPermittedTo('read', { checkOwnership: true }, sessionOwnerFn),
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
                    dataset: {
                      include: {
                        workflows: true,
                      },
                    },
                  },
                },
              },
            },
          },
          orderBy: { order: 'asc' },
        },
        session_workflows: {
          select: {
            workflow_id: true,
            created_at: true,
            initiator: {
              select: { id: true, username: true, name: true },
            },
          },
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

    // Increment access count
    await prisma.genome_browser_session.update({
      where: { id },
      data: { access_count: { increment: 1 } },
    });

    // Enrich workflows with Rhythm data for accurate status checking
    const enrichedWorkflowsByDataset = {};
    const datasetMap = new Map();
    session.session_tracks.forEach((st) => {
      const dataset = st.track?.dataset_file?.dataset;
      if (dataset) {
        datasetMap.set(dataset.id, dataset);
      }
    });

    // Fetch enriched workflow data from Rhythm for each dataset
    for (const [datasetId, dataset] of datasetMap) {
      if (dataset.workflows && dataset.workflows.length > 0) {
        try {
          const wf_res = await wfService.getAll({
            workflow_ids: dataset.workflows.map((x) => x.id),
          });
          enrichedWorkflowsByDataset[datasetId] = wf_res.data.results || [];
        } catch (error) {
          logger.warn(`Failed to fetch workflow details for dataset ${datasetId}`, error);
          enrichedWorkflowsByDataset[datasetId] = [];
        }
      }
    }

    // Fetch workflow status from Rhythm for session workflows
    if (session.session_workflows && session.session_workflows.length > 0) {
      try {
        const sessionWorkflowIds = session.session_workflows.map((sw) => sw.workflow_id);
        const wf_res = await wfService.getAll({
          workflow_ids: sessionWorkflowIds,
        });

        // Enrich session_workflows with full Rhythm workflow data, preserving
        // the DB-sourced initiator which Rhythm does not know about.
        session.session_workflows = session.session_workflows.map((sw) => {
          const enrichedWf = wf_res.data.results.find((w) => w.id === sw.workflow_id);
          if (enrichedWf) {
            return { ...enrichedWf, initiator: sw.initiator };
          }
          return { id: sw.workflow_id, created_at: sw.created_at, status: null, name: null, initiator: sw.initiator };
        });
      } catch (error) {
        logger.warn(`Failed to fetch workflow status for session ${id}`, error);
        // Keep session_workflows but without status enrichment
      }
    }

    // Evaluate data request status dynamically with enriched workflows
    const dataRequestStatus = evaluateDataRequestStatus(session, enrichedWorkflowsByDataset);

    res.json({
      ...session,
      data_requested: dataRequestStatus,
    });
  }),
);

// PATCH /sessions/:id - Update a session
router.patch(
  '/:id',
  isPermittedTo('update', { checkOwnership: true }, sessionOwnerFn),
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
    body('metadata').isObject().optional(),
  ],
  asyncHandler(async (req, res) => {
    const { id } = req.params;
    const {
      title, genome, genome_type, is_public, track_ids, metadata,
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

    // Validate tracks if provided
    if (track_ids && track_ids.length > 0) {
      let tracks;

      if (userCanAccessAll(req.user)) {
        // Admin/operator can access all tracks
        tracks = await prisma.track.findMany({
          where: { id: { in: track_ids } },
        });
      } else {
        // User role can only access tracks from projects they are a member of
        tracks = await prisma.track.findMany({
          where: {
            id: { in: track_ids },
            dataset_file: {
              dataset: {
                projects: {
                  some: {
                    project: {
                      users: { some: { user_id: req.user.id } },
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
      const validationResult = validateTracksForSession(tracks);
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
    if (metadata !== undefined) {
      updateData.metadata = {
        ...(existingSession.metadata || {}),
        ...metadata,
      };
    }

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
  isPermittedTo('delete', { checkOwnership: true }, sessionOwnerFn),
  [param('id').isInt().toInt()],
  asyncHandler(async (req, res) => {
    const { id } = req.params;

    const existingSession = await prisma.genome_browser_session.findUnique({
      where: { id },
      select: { id: true, user_id: true },
    });

    if (!existingSession) {
      return res.status(404).json({ error: 'Session not found' });
    }

    // Delete session (cascade will handle related records)
    await prisma.genome_browser_session.delete({
      where: { id },
    });

    res.status(204).send();
  }),
);

// POST /sessions/:id/set-file-cookie - Set authentication cookie for file access
// Called by frontend before initializing genome browser (IGV or WashU)
router.post(
  '/:id/set-file-cookie',
  isPermittedTo('read', { checkOwnership: true }, sessionOwnerFn),
  [param('id').isInt().toInt()],
  asyncHandler(async (req, res) => {
    const sessionId = req.params.id;

    // Verify session exists and user has access
    const session = await prisma.genome_browser_session.findUnique({
      where: { id: sessionId },
      select: { id: true, user_id: true },
    });

    if (!session) {
      return res.status(404).json({ error: 'Session not found' });
    }

    // Get the JWT token from the Authorization header
    const authHeader = req.headers.authorization || '';
    if (!authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'No authorization token provided' });
    }
    const token = authHeader.split(' ')[1];

    // Set the auth cookie for file exposure
    // Cookie is scoped to this specific session's file exposure endpoint
    res.cookie('bioloop_auth', token, {
      httpOnly: true,
      secure: config.get('mode') === 'production', // HTTPS only in production
      sameSite: 'lax',
      path: `/api/sessions/${sessionId}/files/expose`,
      maxAge: 15 * 60 * 1000, // 15 minutes
    });

    res.json({ message: 'Cookie set successfully' });
  }),
);

// GET /sessions/:id/datahub - Export session tracks in genome browser format
// Supports both IGV and WashU browsers via ?browser=igv|washu query parameter
// Returns track configurations with relative URLs for same-origin file access
router.get(
  '/:id/datahub',
  isPermittedTo('read', { checkOwnership: true }, sessionOwnerFn),
  [
    param('id').isInt().toInt(),
    query('browser')
      .optional()
      .isIn(Object.values(BROWSER_TYPES))
      .default(BROWSER_TYPES.IGV),
  ],
  asyncHandler(async (req, res) => {
    const sessionId = req.params.id;
    const browserType = req.query.browser || BROWSER_TYPES.IGV;

    logger.info(`[DATAHUB] Request for session ${sessionId}, browser: ${browserType}`);

    const session = await prisma.genome_browser_session.findUnique({
      where: { id: sessionId },
      select: {
        id: true,
        user_id: true,
        genome: true,
        session_tracks: {
          include: {
            track: {
              include: {
                dataset_file: {
                  include: {
                    dataset: {
                      select: {
                        id: true,
                        type: true,
                        metadata: true,
                        staged_path: true,
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

    // Collect all unique dataset IDs to fetch their files (for finding index files)
    const datasetIds = [...new Set(session.session_tracks.map((st) => st.track.dataset_file.dataset.id))];

    // Fetch all dataset files for these datasets (to find index files)
    const allDatasetFiles = await prisma.dataset_file.findMany({
      where: {
        dataset_id: { in: datasetIds },
      },
      select: {
        id: true,
        path: true,
        metadata: true,
        dataset_id: true,
        dataset: {
          select: {
            id: true,
            type: true,
            metadata: true,
            staged_path: true,
          },
        },
      },
    });

    // Group dataset files by dataset_id for easier lookup
    const filesByDataset = allDatasetFiles.reduce((acc, file) => {
      if (!acc[file.dataset_id]) {
        acc[file.dataset_id] = [];
      }
      acc[file.dataset_id].push(file);
      return acc;
    }, {});

    // Get genome reference (e.g., "hg38", "hg19", "mm10")
    // Try to infer from session tracks, fallback to session.genome, finally default to hg38
    // const firstTrack = session.session_tracks[0];
    // const firstDataset = firstTrack?.track?.dataset_file?.dataset;
    // const genomeInfo = firstDataset?.genomic_details;
    const { genome } = session;
    //  || 'hg38'

    // Serialize tracks based on browser type
    if (browserType === BROWSER_TYPES.WASHU) {
      // WashU format: Array of track objects
      const tracks = session.session_tracks
        .map((st) => serializeTrackForWashU(st, sessionId, filesByDataset))
        .filter(Boolean);

      logger.info(`[DATAHUB] Returning ${tracks.length} WashU tracks for session ${sessionId}`);

      // WashU expects genome and tracks (similar to IGV but different track format)
      res.json({ genome, tracks });
    } else {
      // IGV format: Object with genome + tracks
      const tracks = session.session_tracks
        .map((st) => serializeTrackForIGV(st, sessionId, filesByDataset))
        .filter(Boolean);

      logger.info(`[DATAHUB] Returning ${tracks.length} IGV tracks for session ${sessionId}`);

      res.json({ genome, tracks });
    }
  }),
);

// OPTIONS /sessions/:id/files/expose/* - Handle CORS preflight for genome browsers
fileExposureRouter.options('/:id/files/expose/*', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Range, Content-Type, Authorization, Cookie');
  res.set('Access-Control-Max-Age', '86400'); // 24 hours
  res.status(204).send();
});

// GET /sessions/:id/files/expose/* - Expose genomic files for genome browsers (IGV, WashU)
// Authenticated via HttpOnly cookie, scoped to this session
// Supports HTTP Range requests for efficient file streaming
// This route is mounted BEFORE global authentication to use cookie-based auth
fileExposureRouter.get(
  '/:id/files/expose/*',
  authenticateWithCookie, // Cookie-based auth
  [param('id').isInt().toInt()],
  asyncHandler(async (req, res, next) => {
    const sessionId = req.params.id;
    const requestedPath = req.params[0] || ''; // Everything after /files/expose/

    logger.info('[FILE EXPOSE] Request received');
    console.dir({
      sessionId,
      requestedPath,
      user: req.user?.username,
      userId: req.user?.id,
      rangeHeader: req.headers.range || 'none',
    }, { depth: null });

    // Verify user has access to this session
    const session = await prisma.genome_browser_session.findUnique({
      where: { id: sessionId },
      select: {
        id: true,
        user_id: true,
        session_tracks: {
          include: {
            track: {
              include: {
                dataset_file: {
                  include: {
                    dataset: {
                      select: {
                        id: true,
                        type: true,
                        metadata: true,
                        staged_path: true,
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
      logger.warn('[FILE EXPOSE] Session not found', { sessionId });
      return next(createError.NotFound('Session not found'));
    }

    logger.info('[FILE EXPOSE] Session found');
    console.dir({
      sessionId,
      sessionUserId: session.user_id,
      trackCount: session.session_tracks.length,
    }, { depth: null });

    // Check if user owns the session or has admin/operator access
    if (!userCanAccessAll(req.user) && session.user_id !== req.user.id) {
      logger.warn('[FILE EXPOSE] Access denied', {
        sessionId,
        sessionUserId: session.user_id,
        requestUserId: req.user.id,
      });
      return next(createError.Forbidden('Access denied to this session'));
    }

    // Verify the requested file belongs to this session's tracks
    const cleanedRequestedPath = requestedPath.replace(/^\/+/, '');
    logger.info('[FILE EXPOSE] Cleaned requested path');
    console.dir({ cleanedRequestedPath }, { depth: null });

    const matchedTrack = session.session_tracks.find((st) => {
      const { dataset } = st.track.dataset_file;
      const datasetFile = st.track.dataset_file;
      const relativePath = pathResolver.getRelativeFilePath({ dataset, datasetFile });
      const cleanedRelativePath = relativePath.replace(/^\/+/, '');
      return cleanedRelativePath === cleanedRequestedPath;
    });

    if (!matchedTrack) {
      logger.warn('[FILE EXPOSE] File not found in session tracks', {
        sessionId,
        requestedPath: cleanedRequestedPath,
        availableTracks: session.session_tracks.map((st) => {
          const { dataset } = st.track.dataset_file;
          const datasetFile = st.track.dataset_file;
          return pathResolver.getRelativeFilePath({ dataset, datasetFile });
        }),
      });
      return next(createError.NotFound('File not found in this session'));
    }

    const matchedFile = matchedTrack.track.dataset_file;
    const matchedDataset = matchedTrack.track.dataset_file.dataset;

    logger.info('[FILE EXPOSE] File matched in session');
    console.dir({
      sessionId,
      trackId: matchedTrack.track.id,
      datasetId: matchedDataset.id,
      fileId: matchedFile.id,
      fileName: matchedFile.name,
    }, { depth: null });

    // Construct full file path using pathResolver service
    const relativePath = pathResolver.getRelativeFilePath({
      dataset: matchedDataset,
      datasetFile: matchedFile,
    });
    logger.info('[FILE EXPOSE] Relative path with dataset_type');
    console.dir({ relativePath }, { depth: null });

    // Resolve to absolute path accessible by the container
    const resolvedFull = pathResolver.resolveToAbsolutePath(relativePath);
    logger.info('[FILE EXPOSE] Full absolute path constructed');
    console.dir({ resolvedFull }, { depth: null });

    // Security: Verify path starts with access root to prevent path traversal
    const accessRoot = pathResolver.getFileAccessRoot();
    const resolvedRoot = path.resolve(accessRoot);
    if (!resolvedFull.startsWith(resolvedRoot)) {
      logger.error('[FILE EXPOSE] Path traversal attempt detected');
      console.dir({
        resolvedFull,
        resolvedRoot,
        accessRoot,
      }, { depth: null });
      return next(createError.BadRequest('Invalid file path'));
    }

    // Check file exists (will throw if not found)
    await fs.promises.access(resolvedFull, fs.constants.F_OK);

    // Get file stats
    const fileStats = await fs.promises.stat(resolvedFull);
    logger.info('[FILE EXPOSE] File exists and accessible');
    console.dir({
      resolvedFull,
      fileSize: fileStats.size,
      fileSizeFormatted: `${(fileStats.size / 1024 / 1024).toFixed(2)} MB`,
      isFile: fileStats.isFile(),
      isDirectory: fileStats.isDirectory(),
    }, { depth: null });

    // Set headers
    const ext = path.extname(resolvedFull).toLowerCase();
    logger.info('[FILE EXPOSE] File extension extracted');
    console.dir({ ext }, { depth: null });

    const mimeTypes = {
      '.bam': 'application/octet-stream',
      '.bai': 'application/octet-stream',
      '.bw': 'application/octet-stream',
      '.bigwig': 'application/octet-stream',
      '.vcf': 'text/plain',
      '.gz': 'application/gzip',
      '.bed': 'text/plain',
      '.gff': 'text/plain',
      '.gff3': 'text/plain',
      '.gtf': 'text/plain',
    };
    const contentType = mimeTypes[ext] || 'application/octet-stream';
    logger.info('[FILE EXPOSE] Content type determined');
    console.dir({ contentType }, { depth: null });

    res.set('Content-Type', contentType);
    res.set('Accept-Ranges', 'bytes');
    res.set('Access-Control-Allow-Origin', '*');
    res.set('Access-Control-Expose-Headers', 'Content-Range, Accept-Ranges, Content-Length');

    // CRITICAL: Disable compression for binary genomic files
    // BigWig/BAM files must be served as raw bytes for proper parsing by genome browsers
    // Multiple strategies to prevent compression at different layers:

    // 1. Tell Express compression middleware to skip (if used)
    res.locals.compress = false;

    // 2. Tell Nginx to not buffer/compress this response
    // res.set('X-Accel-Buffering', 'no');

    // 3. Explicitly set Content-Encoding (some proxies respect this)
    res.set('Content-Encoding', 'identity');

    // 4. Remove Vary header that triggers compression
    res.removeHeader('Vary');

    // Handle Range requests (required for IGV)
    const { range } = req.headers;
    if (range) {
      logger.info('[FILE EXPOSE] Range header detected');
      console.dir({ range }, { depth: null });

      const stats = await fs.promises.stat(resolvedFull);
      const fileSize = stats.size;
      logger.info('[FILE EXPOSE] File size from stats');
      console.dir({ fileSize }, { depth: null });

      const parts = range.replace(/bytes=/, '').split('-');
      logger.info('[FILE EXPOSE] Range parts parsed');
      console.dir({ parts }, { depth: null });

      const start = parseInt(parts[0], 10);
      logger.info('[FILE EXPOSE] Range start position');
      console.dir({ start }, { depth: null });

      const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;
      logger.info('[FILE EXPOSE] Range end position');
      console.dir({ end }, { depth: null });

      const chunksize = (end - start) + 1;
      logger.info('[FILE EXPOSE] Chunk size calculated');
      console.dir({ chunksize }, { depth: null });

      const percentOfFile = ((chunksize / fileSize) * 100).toFixed(2);
      logger.info('[FILE EXPOSE] Percent of file calculated');
      console.dir({ percentOfFile }, { depth: null });

      logger.info('[FILE EXPOSE] Streaming range request');
      console.dir({
        resolvedFull,
        rangeHeader: range,
        start,
        end,
        chunksize,
        fileSize,
        percentOfFile: `${percentOfFile}%`,
      }, { depth: null });

      res.status(206); // Partial Content
      res.set('Content-Range', `bytes ${start}-${end}/${fileSize}`);
      res.set('Content-Length', chunksize);

      const fileStream = fs.createReadStream(resolvedFull, { start, end });
      logger.info('[FILE EXPOSE] File stream created (range)');
      console.dir({ streamOptions: { start, end } }, { depth: null });

      fileStream.on('open', () => {
        logger.info('[FILE EXPOSE] Stream opened (range)');
        console.dir({ start, end }, { depth: null });
      });

      fileStream.on('error', (err) => {
        logger.error('[FILE EXPOSE] Error streaming file range', {
          error: err.message,
          resolvedFull,
          start,
          end,
        });
        if (!res.headersSent) {
          next(createError.InternalServerError('Error streaming file'));
        }
      });

      fileStream.on('end', () => {
        logger.info('[FILE EXPOSE] Stream completed (range)');
        console.dir({
          resolvedFull,
          start,
          end,
          bytesStreamed: chunksize,
        }, { depth: null });
      });

      fileStream.pipe(res);
    } else {
      // Stream full file
      logger.info('[FILE EXPOSE] Streaming full file');
      console.dir({
        resolvedFull,
        fileSize: fileStats.size,
      }, { depth: null });

      const fileStream = fs.createReadStream(resolvedFull);
      logger.info('[FILE EXPOSE] File stream created (full)');
      console.dir({ resolvedFull }, { depth: null });

      fileStream.on('open', () => {
        logger.info('[FILE EXPOSE] Stream opened (full file)');
      });

      fileStream.on('error', (err) => {
        logger.error('[FILE EXPOSE] Error streaming file', {
          error: err.message,
          resolvedFull,
        });
        if (!res.headersSent) {
          next(createError.InternalServerError('Error streaming file'));
        }
      });

      fileStream.on('end', () => {
        logger.info('[FILE EXPOSE] Stream completed (full file)');
        console.dir({
          resolvedFull,
          bytesStreamed: fileStats.size,
        }, { depth: null });
      });

      fileStream.pipe(res);
    }
  }),
);

// GET /sessions/:id/datasets - Get datasets for a session with optional staging filter
// For legacy sessions, uses metadata.datasets (CMG dataproduct IDs) to find associated datasets
router.get(
  '/:id/datasets',
  isPermittedTo('read', { checkOwnership: true }, sessionOwnerFn),
  [
    param('id').isInt().toInt(),
    query('staged').optional().isBoolean().toBoolean(),
  ],
  asyncHandler(async (req, res) => {
    const sessionId = req.params.id;
    const stagedFilter = req.query.staged;

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
                        workflows: true,
                        states: true,
                      },
                    },
                  },
                },
              },
            },
          },
        },
        session_workflows: {
          select: {
            workflow_id: true,
          },
        },
      },
    });

    if (!session) {
      return res.status(404).json({ error: 'Session not found' });
    }

    let datasets = [];
    const datasetMap = new Map(); // Track unique datasets

    // Helper function to check if hydration workflow has completed successfully
    const isHydrationComplete = async (sessionWorkflows) => {
      if (!sessionWorkflows || sessionWorkflows.length === 0) {
        return false;
      }

      // Fetch workflow details from Rhythm to get workflow names and statuses
      try {
        const wf_res = await wfService.getAll({
          workflow_ids: sessionWorkflows.map((sw) => sw.workflow_id),
        });

        // Find the hydrate_session workflow by NAME (not by UUID workflow_id)
        const hydrationWorkflow = wf_res.data.results.find(
          (wf) => wf.name === WORKFLOWS.HYDRATE_SESSION
        );

        if (!hydrationWorkflow) {
          return false;
        }

        // Check if workflow status is SUCCESS (DONE_STATUSES = ['REVOKED', 'FAILURE', 'SUCCESS'])
        return hydrationWorkflow.status === 'SUCCESS';
      } catch (error) {
        logger.warn(`Failed to fetch hydration workflow status for session ${sessionId}`, error);
        // If can't fetch status, assume not hydrated
        return false;
      }
    };

    const isLegacy = legacyMigrationService.isLegacySession(session);
    const needsHydration = isLegacy && !(await isHydrationComplete(session.session_workflows));

    // If legacy AND not hydrated: fetch from BOTH sources
    if (isLegacy && needsHydration && session.metadata?.datasets && Array.isArray(session.metadata.datasets)) {
      logger.info(
        `[SESSIONS] Legacy session ${sessionId} not hydrated - ` +
        `fetching from BOTH metadata.datasets and session_tracks`
      );

      // SOURCE 1: metadata.datasets (legacy datasets from CMG)
      const cmgDataproductIds = session.metadata.datasets;
      for (const cmgId of cmgDataproductIds) {
        const dataset = await prisma.dataset.findFirst({
          where: { cmg_id: cmgId },
          include: {
            genomic_details: true,
            workflows: true,
            states: true,
          },
        });

        if (dataset) {
          const migrationStatus = await legacyMigrationService.getDatasetMigrationStatus(dataset.id);
          dataset.migration_status = migrationStatus;

          if (stagedFilter === undefined || dataset.is_staged === stagedFilter) {
            datasetMap.set(dataset.id, dataset); // Use Map to avoid duplicates
          }
        } else {
          logger.warn(`[SESSIONS] Dataset with CMG ID ${cmgId} not found for session ${sessionId}`);
        }
      }

      // SOURCE 2: session_tracks (newly added tracks before hydration)
      session.session_tracks.forEach((st) => {
        const dataset = st.track.dataset_file?.dataset;
        if (dataset) {
          if (stagedFilter === undefined || dataset.is_staged === stagedFilter) {
            datasetMap.set(dataset.id, dataset); // Add to map (won't duplicate)
          }
        }
      });

      datasets = Array.from(datasetMap.values());
    } else {
      // Non-legacy OR legacy+hydrated: use ONLY session_tracks
      logger.info(`[SESSIONS] Fetching datasets for session ${sessionId} from session_tracks only`);

      session.session_tracks.forEach((st) => {
        const dataset = st.track.dataset_file?.dataset;
        if (dataset) {
          if (stagedFilter === undefined || dataset.is_staged === stagedFilter) {
            datasetMap.set(dataset.id, dataset);
          }
        }
      });

      datasets = Array.from(datasetMap.values());
    }

    res.json({
      count: datasets.length,
      datasets,
    });
  }),
);

// GET /sessions/:id/projects - Get projects associated with a session
router.get(
  '/:id/projects',
  isPermittedTo('read', { checkOwnership: true }, sessionOwnerFn),
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

    // First, get the session to verify it exists and user has access
    let sessionWhere;
    if (userCanAccessAll(req.user)) {
      sessionWhere = { id };
    } else {
      sessionWhere = { id, user_id: req.user.id };
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
  }),
);

// Track validation functions
const validateTracksForSession = (tracks) => {
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

  // Validation 3: REMOVED - Allow users to specify any genome type/assembly
  // Users can override genome values in the UI regardless of track dataset values

  return { isValid: true };
};

router.get(
  '/:id/tracks',
  isPermittedTo('read', { checkOwnership: true }, sessionOwnerFn),
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

    if (!session) {
      return res.status(404).json({ error: 'Session not found' });
    }

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

//  Launch a workflow on the session - UI
router.post(
  '/:id/workflows/:wf',
  isPermittedTo('update', { checkOwnership: true }, sessionOwnerFn),
  validate([
    param('id').isInt().toInt(),
    param('wf').isIn([CONSTANTS.WORKFLOWS.HYDRATE_SESSION]),
  ]),
  asyncHandler(async (req, res) => {
    const sessionId = req.params.id;
    const wfName = req.params.wf;

    const session = await prisma.genome_browser_session.findUnique({
      where: { id: sessionId },
      select: { id: true, user_id: true },
    });

    if (!session) {
      return res.status(404).json({ error: 'Session not found' });
    }

    logger.info(`User ${req.user.id} starting workflow ${wfName} on session ${sessionId}`);

    // Build workflow body using same pattern as datasets
    const workflowConfig = config.get('workflow_registry')[wfName];

    if (!workflowConfig) {
      logger.error(`Workflow ${wfName} not found in workflow_registry`);
      return res.status(500).json({ error: 'Workflow configuration not found' });
    }

    const defaultQueue = config.get('default_queue') || `${config.get('app_id')}.q`;
    const wfBody = {
      ...workflowConfig,
      name: wfName,
      app_id: config.get('app_id'),
      steps: workflowConfig.steps.map((step) => ({
        ...step,
        queue: step.queue || defaultQueue,
      })),
    };

    // Create the workflow via workflow service (Rhythm generates the workflow ID)
    let wf;
    try {
      wf = (await wfService.create({
        ...wfBody,
        args: [sessionId],
      })).data;
      logger.info(`Workflow ${wf.workflow_id} created successfully for session ${sessionId}`);
    } catch (error) {
      logger.error(`Failed to create workflow for session ${sessionId}:`, error);
      return res.status(500).json({ error: 'Failed to create workflow' });
    }

    // Create session_workflow association using Rhythm-generated ID
    await prisma.session_workflow.create({
      data: {
        session_id: sessionId,
        workflow_id: wf.workflow_id,
        initiator_id: req.user.id,
      },
    });

    return res.json(wf);
  }),
);

// Export main router for authenticated routes
module.exports = router;

// Export file exposure router separately - this needs to be mounted BEFORE global authenticate middleware
// to allow cookie-based authentication
module.exports.fileExposureRouter = fileExposureRouter;

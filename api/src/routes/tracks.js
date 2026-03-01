const express = require('express');
const { query, param, body } = require('express-validator');
const config = require('config');
const { Prisma } = require('@prisma/client');
const prisma = require('@/db');
const asyncHandler = require('../middleware/asyncHandler');
const { accessControl } = require('../middleware/auth');
const { has_project_assoc } = require('../services/project');

const router = express.Router();

const normalizeFileTypeFilter = (fileTypeParam) => {
  if (!fileTypeParam) return null;
  if (Array.isArray(fileTypeParam)) {
    return {
      in: fileTypeParam,
    };
  }
  if (typeof fileTypeParam === 'string' && fileTypeParam.includes(',')) {
    const fileTypes = fileTypeParam.split(',').map((ft) => ft.trim()).filter(Boolean);
    if (fileTypes.length === 0) {
      return null;
    }
    return {
      in: fileTypes,
    };
  }
  return fileTypeParam;
};

const mergeDatasetFilter = (filterQuery, datasetCondition) => {
  filterQuery.dataset_file = {
    ...(filterQuery.dataset_file || {}),
    dataset: {
      ...(filterQuery.dataset_file?.dataset || {}),
      ...datasetCondition,
    },
  };
};

const attachDatasetAnalysisType = (track) => {
  if (track) {
    const analysisType = track.dataset_file?.dataset?.analysis_type?.name ?? null;
    track.analysis_type = analysisType;
  }
  return track;
};

// Middleware to check permissions
const isPermittedTo = accessControl('tracks');

router.get(
  '/',
  isPermittedTo('read'),
  [
    query('project_id').trim().optional(),
    query('dataset_file_id').isInt().toInt().optional(),
    query('name').trim().optional(),
    query('file_type').trim().optional(),
    query('browser_compatible').isBoolean().toBoolean().optional(),
    query('genome_type').trim().optional(),
    query('genome_value').trim().optional(),
    query('limit').isInt({ min: 1 }).toInt().optional(),
    query('offset').isInt({ min: 0 }).toInt().optional(),
    query('sort_by').default('created_at'),
    query('sort_order').default('desc').isIn(['asc', 'desc']),
  ],
  asyncHandler(async (req, res) => {
    const {
      project_id, dataset_file_id, name, file_type, genome_type, genome_value, limit, offset, sort_by, sort_order,
    } = req.query;

    try {
      // Build filter query based on user's project access
      const filter_query = {

      };

      // If user has admin/operator role, they can see all tracks.
      // Otherwise, filter by user's project membership through datasets
      if (!req.permission.granted) {
        // Get user's project memberships
        const userProjects = await prisma.project_user.findMany({
          where: { user_id: req.user.id },
          select: { project_id: true },
        });

        const projectIds = userProjects.map((p) => p.project_id);

        // Filter tracks by datasets that belong to user's projects
        filter_query.dataset_file = {
          dataset: {
            projects: {
              some: {
                project_id: { in: projectIds },
              },
            },
          },
        };
      }

      // If asking for a specific project, verify that user has access to it
      if (project_id) {
        // Verify user has access to this project
        if (!req.permission.granted) {
          const hasAccess = await has_project_assoc({
            projectId: project_id,
            userId: req.user.id,
          });

          if (!hasAccess) {
            return res.status(403).json({ error: 'Access denied to specified project' });
          }
        }

        // Filter tracks by datasets that belong to the specified project
        filter_query.dataset_file = {
          dataset: {
            projects: {
              some: {
                project_id,
              },
            },
          },
        };
      }

      // optional filters
      if (dataset_file_id) {
        filter_query.dataset_file_id = dataset_file_id;
      }

      if (name) {
        filter_query.name = {
          contains: name,
          mode: 'insensitive',
        };
      }

      const normalizedFileTypeFilter = normalizeFileTypeFilter(file_type);
      if (normalizedFileTypeFilter) {
        const nameFilter = typeof normalizedFileTypeFilter === 'object' && normalizedFileTypeFilter.in
          ? { in: normalizedFileTypeFilter.in, mode: 'insensitive' }
          : { equals: normalizedFileTypeFilter, mode: 'insensitive' };
        
        mergeDatasetFilter(filter_query, {
          analysis_type: {
            name: nameFilter,
          },
        });
      }

      if (genome_type || genome_value) {
        const genomicDetailsFilter = {};
        if (genome_type) genomicDetailsFilter.genome_type = genome_type;
        if (genome_value) genomicDetailsFilter.genome_value = genome_value;

        mergeDatasetFilter(filter_query, {
          genomic_details: genomicDetailsFilter,
        });
      }

      // Filter by PRIMARY role if genome browser feature is enabled
      const isGenomeBrowserEnabled = config.get('enabled_features.genome_browser');
      if (isGenomeBrowserEnabled) {
        filter_query.dataset_file = {
          ...(filter_query.dataset_file || {}),
          metadata: {
            path: ['role'],
            equals: 'PRIMARY',
          },
        };
      }

      const [tracks, count] = await prisma.$transaction([
        prisma.track.findMany({
          where: filter_query,
          include: {
            dataset_file: {
              select: {
                id: true,
                name: true,
                path: true,
                size: true,
                filetype: true,
                metadata: true,
                dataset: {
                  select: {
                    id: true,
                    name: true,
                    type: true,
                    metadata: true,
                    genomic_details: true,
                    analysis_type: true,
                    projects: {
                      select: {
                        project: {
                          select: {
                            id: true,
                            name: true,
                          },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
          skip: offset ?? Prisma.skip,
          take: limit ?? Prisma.skip,
          orderBy: {
            [sort_by]: sort_order,
          },
        }),
        prisma.track.count({
          where: filter_query,
        }),
      ]);

      tracks.forEach(attachDatasetAnalysisType);

      res.status(200).json({
        metadata: { count },
        tracks,
      });
    } catch (error) {
      console.error(error);
      res.status(500).json({ error: 'Failed to fetch tracks' });
    }
  }),
);

// Create a new track
router.post(
  '/',
  isPermittedTo('create'),
  [
    body('name').isString().notEmpty().trim(),
    body('dataset_file_id').isInt().toInt(),
  ],
  asyncHandler(async (req, res) => {
    const {
      name, dataset_file_id,
    } = req.body;

    try {
      // Verify the dataset file exists and user has access to it
      const datasetFile = await prisma.dataset_file.findUnique({
        where: { id: dataset_file_id },
        include: {
          dataset: {
            include: {
              projects: true,
            },
          },
        },
      });

      if (!datasetFile) {
        return res.status(404).json({ error: 'Dataset file not found' });
      }

      // Check if User has access to the Dataset whose File the Track being
      // created is associated with.
      if (!req.permission.granted) {
        const hasAccess = await has_project_assoc({
          projectId: datasetFile.dataset.projects[0]?.project_id,
          userId: req.user.id,
        });

        if (!hasAccess) {
          return res.status(403).json({ error: 'Access denied to dataset' });
        }
      }

      // Create the track
      const track = await prisma.track.create({
        data: {
          name,
          dataset_file_id,
        },
        include: {
          dataset_file: {
            select: {
              id: true,
              name: true,
              path: true,
              size: true,
              filetype: true,
              dataset: {
                select: {
                  id: true,
                  name: true,
                  type: true,
                  metadata: true,
                  projects: {
                    select: {
                      project: {
                        select: {
                          id: true,
                          name: true,
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

      if (file_type) {
        const datasetId = datasetFile.dataset?.id || datasetFile.dataset_id;
        if (datasetId) {
          // Find or create analysis_type with the given name (case-insensitive)
          const formattedName = file_type.toUpperCase().replace(/\s+/g, '_').replace(/[^A-Z0-9_]/g, '');
          const formattedExtension = `.${file_type.toLowerCase()}`;
          
          let analysisType = await prisma.analysis_type.findFirst({
            where: {
              AND: [
                {
                  name: {
                    equals: formattedName,
                    mode: 'insensitive',
                  },
                },
                {
                  extension: {
                    equals: formattedExtension,
                    mode: 'insensitive',
                  },
                },
              ],
            },
          });
          
          if (!analysisType) {
            analysisType = await prisma.analysis_type.create({
              data: {
                name: formattedName,
                extension: formattedExtension,
              },
            });
          }

          await prisma.dataset.update({
            where: { id: datasetId },
            data: {
              analysis_type: {
                connect: { id: analysisType.id },
              },
            },
          });
          
          if (track.dataset_file?.dataset) {
            track.dataset_file.dataset.analysis_type = analysisType;
          }
        }
      }

      attachDatasetAnalysisType(track);

      res.status(201).json(track);
    } catch (error) {
      console.error(error);
      res.status(500).json({ error: 'Failed to create track' });
    }
  }),
);

// GET /tracks/:id - Get a specific track
router.get(
  '/:id',
  isPermittedTo('read'),
  [
    param('id').isInt().toInt(),
    query('include_dataset').isBoolean().toBoolean().optional(),
  ],
  asyncHandler(async (req, res) => {
    const { id } = req.params;

    const include = {
      dataset_file: {
        include: {
          dataset: {
            include: {
              projects: {
                include: {
                  project: {
                    select: {
                      id: true,
                      name: true,
                      slug: true,
                    },
                  },
                },
              },
            },
          },
        },
      },
      session_tracks: {
        include: {
          session: {
            include: {
              user: {
                select: {
                  id: true,
                  username: true,
                  name: true,
                },
              },
            },
          },
        },
        orderBy: {
          order: 'asc',
        },
      },
    };

    // If user has admin/operator role, they can see all tracks
    // Otherwise, filter by user's project membership through datasets
    let track;

    if (req.permission.granted) {
      // Admin/operator can see any track
      track = await prisma.track.findFirst({
        where: { id },
        include,
      });
    } else {
      // Regular users can only see tracks from datasets they have access to
      track = await prisma.track.findFirst({
        where: {
          id,
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
        include,
      });
    }

    if (!track) {
      return res.status(404).json({ error: 'Track not found or access denied' });
    }

    attachDatasetAnalysisType(track);

    res.json(track);
  }),
);

// Update a track
router.patch(
  '/:id',
  isPermittedTo('update'),
  [
    param('id').isInt().toInt(),
    body('name').isString().optional().trim(),
  ],
  asyncHandler(async (req, res) => {
    const { id } = req.params;
    const {
      name,
    } = req.body;

    try {
      // Check if track exists and user has access
      const existingTrack = await prisma.track.findUnique({
        where: { id },
        include: {
          dataset_file: {
            select: {
              dataset: {
                select: {
                  id: true,
                  projects: {
                    select: {
                      project_id: true,
                    },
                  },
                },
              },
            },
          },
        },
      });

      if (!existingTrack) {
        return res.status(404).json({ error: 'Track not found' });
      }

      // Check access control
      if (!req.permission.granted) {
        const hasAccess = existingTrack.dataset_file.dataset.projects.some((pt) => has_project_assoc({
          projectId: pt.project_id,
          userId: req.user.id,
        }));

        if (!hasAccess) {
          return res.status(403).json({ error: 'Access denied to track' });
        }
      }

      // Update the track
      const updateData = {};
      if (name !== undefined) updateData.name = name;

      const track = await prisma.track.update({
        where: { id },
        data: updateData,
        include: {
          dataset_file: {
            select: {
              id: true,
              name: true,
              path: true,
              size: true,
              filetype: true,
              dataset: {
                select: {
                  id: true,
                  name: true,
                  type: true,
                  metadata: true,
                  projects: {
                    select: {
                      project: {
                        select: {
                          id: true,
                          name: true,
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

      if (file_type !== undefined) {
        const datasetId = existingTrack.dataset_file?.dataset?.id;
        if (datasetId) {
          // Find or create analysis_type with the given name (case-insensitive)
          const formattedName = file_type.toUpperCase().replace(/\s+/g, '_').replace(/[^A-Z0-9_]/g, '');
          const formattedExtension = `.${file_type.toLowerCase()}`;
          
          let analysisType = await prisma.analysis_type.findFirst({
            where: {
              AND: [
                {
                  name: {
                    equals: formattedName,
                    mode: 'insensitive',
                  },
                },
                {
                  extension: {
                    equals: formattedExtension,
                    mode: 'insensitive',
                  },
                },
              ],
            },
          });
          
          if (!analysisType) {
            analysisType = await prisma.analysis_type.create({
              data: {
                name: formattedName,
                extension: formattedExtension,
              },
            });
          }

          await prisma.dataset.update({
            where: { id: datasetId },
            data: {
              analysis_type: {
                connect: { id: analysisType.id },
              },
            },
          });
          
          if (track.dataset_file?.dataset) {
            track.dataset_file.dataset.analysis_type = analysisType;
          }
        }
      }

      attachDatasetAnalysisType(track);

      res.status(200).json(track);
    } catch (error) {
      console.error(error);
      res.status(500).json({ error: 'Failed to update track' });
    }
  }),
);

// Get tracks for a specific user (ownership-based access control)
router.get(
  '/:username/all',
  isPermittedTo('read', { checkOwnership: true }),
  [
    param('username').isString().notEmpty(),
    query('project_id').isString().optional(),
    query('name').trim().optional(),
    query('file_type').trim().optional(),
    query('browser_compatible').isBoolean().toBoolean().optional(),
    query('genome_type').trim().optional(),
    query('genome_value').trim().optional(),
    query('limit').isInt({ min: 1 }).toInt().optional(),
    query('offset').isInt({ min: 0 }).toInt().optional(),
    query('sort_by').default('created_at'),
    query('sort_order').default('desc').isIn(['asc', 'desc']),
  ],
  asyncHandler(async (req, res) => {
    const { username } = req.params;
    const {
      project_id, name, file_type, genome_type, genome_value, limit, offset, sort_by, sort_order,
    } = req.query;
    const { browser_compatible } = req.query;

    try {
      const user = await prisma.user.findUnique({
        where: { username },
      });
      if (!user) {
        return res.status(404).json({ error: 'User not found' });
      }

      const filter_query = {};

      if (project_id) {
        // Check if the User is part of the specified Project
        const isAssociatedToProject = await has_project_assoc({
          projectId: project_id,
          userId: user.id,
        });

        if (!isAssociatedToProject) {
          return res.status(403).json({ error: 'User is not part of the project' });
        }

        // filter tracks by datasets that belong to the specified project_id
        filter_query.dataset_file = {
          dataset: {
            projects: {
              some: {
                project_id,
              },
            },
          },
        };
      } else {
        // Collect tracks across all Projects that the User belongs to through
        // datasets
        const userProjects = await prisma.project_user.findMany({
          where: { user_id: user.id },
          select: { project_id: true },
        });

        const projectIds = userProjects.map((p) => p.project_id);

        filter_query.dataset_file = {
          dataset: {
            projects: {
              some: {
                project_id: { in: projectIds },
              },
            },
          },
        };
      }

      // Apply optional filters
      if (name) {
        filter_query.name = {
          contains: name,
          mode: 'insensitive',
        };
      }

      const userFileTypeFilter = normalizeFileTypeFilter(file_type);
      if (userFileTypeFilter) {
        const nameFilter = typeof userFileTypeFilter === 'object' && userFileTypeFilter.in
          ? { in: userFileTypeFilter.in, mode: 'insensitive' }
          : { equals: userFileTypeFilter, mode: 'insensitive' };
        
        mergeDatasetFilter(filter_query, {
          analysis_type: {
            name: nameFilter,
          },
        });
      }

      if (genome_type || genome_value) {
        const genomicDetailsFilter = {};
        if (genome_type) genomicDetailsFilter.genome_type = genome_type;
        if (genome_value) genomicDetailsFilter.genome_value = genome_value;

        mergeDatasetFilter(filter_query, {
          genomic_details: genomicDetailsFilter,
        });
      }

      // Filter by PRIMARY role if genome browser feature is enabled
      const isGenomeBrowserEnabledForUser = config.get('enabled_features.genome_browser');
      if (isGenomeBrowserEnabledForUser) {
        filter_query.dataset_file = {
          ...(filter_query.dataset_file || {}),
          metadata: {
            path: ['role'],
            equals: 'PRIMARY',
          },
        };
      }

      const [tracks, count] = await prisma.$transaction([
        prisma.track.findMany({
          where: filter_query,
          include: {
            dataset_file: {
              select: {
                id: true,
                name: true,
                path: true,
                size: true,
                filetype: true,
                metadata: true,
                dataset: {
                  select: {
                    id: true,
                    name: true,
                    type: true,
                    metadata: true,
                    genomic_details: true,
                    analysis_type: true,
                    projects: {
                      select: {
                        project: {
                          select: {
                            id: true,
                            name: true,
                          },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
          skip: offset ?? Prisma.skip,
          take: limit ?? Prisma.skip,
          orderBy: {
            [sort_by]: sort_order,
          },
        }),
        prisma.track.count({
          where: filter_query,
        }),
      ]);

      tracks.forEach(attachDatasetAnalysisType);

      res.status(200).json({
        metadata: { count },
        tracks,
      });
    } catch (error) {
      console.error(error);
      res.status(500).json({ error: 'Failed to fetch tracks' });
    }
  }),
);

module.exports = router;

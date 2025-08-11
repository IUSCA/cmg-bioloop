const express = require('express');
const { PrismaClient } = require('@prisma/client');
const { body, query, param } = require('express-validator');
const asyncHandler = require('../middleware/asyncHandler');
const { accessControl } = require('../middleware/auth');

const prisma = new PrismaClient();

const router = express.Router();

// Middleware to check permissions
const isPermittedTo = accessControl('sessions');

// GET /sessions - Get all sessions accessible to the current user
router.get(
  '/',
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

    // Build filter query
    const filter_query = {
      OR: [
        { user_id: req.user.id }, // User's own sessions
        { is_public: true }, // Public sessions
      ],
    };

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
  '/:username',
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

    // Build filter query
    const filter_query = {
      user_id: targetUser.id,
      OR: [
        { user_id: req.user.id }, // User's own sessions
        { is_public: true }, // Public sessions
      ],
    };

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

// POST /sessions - Create a new session
router.post(
  '/',
  isPermittedTo('create'),
  [
    body('session_name').isString().notEmpty().trim(),
    body('genome').isString().notEmpty().trim(),
    body('genome_type').isString().notEmpty().trim(),
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
    body('is_public').isBoolean().optional(),
  ],
  asyncHandler(async (req, res) => {
    const {
      session_name, genome, genome_type, track_ids = [], is_public = false,
    } = req.body;

    // Validate that all tracks exist and are accessible to the user
    if (track_ids.length > 0) {
      let tracks;

      // If user has admin/operator role, they can access all tracks
      if (req.permission.granted) {
        tracks = await prisma.track.findMany({
          where: {
            id: { in: track_ids },
          },
          include: {
            dataset_file: {
              include: {
                dataset: true,
              },
            },
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
          include: {
            dataset_file: {
              include: {
                dataset: true,
              },
            },
          },
        });
      }

      if (tracks.length !== track_ids.length) {
        return res.status(400).json({ error: 'Some tracks are not accessible' });
      }
    }

    // Create session with tracks
    const session = await prisma.genome_browser_session.create({
      data: {
        title: session_name, // Use session_name for the title
        genome,
        genome_type,
        user_id: req.user.id,
        is_public,
        session_tracks: {
          create: track_ids.map((track_id, index) => ({
            track_id,
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
                    dataset: true,
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
  [param('id').isInt().toInt()],
  asyncHandler(async (req, res) => {
    const { id } = req.params;

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

    // Check access permissions
    const hasAccess = session.user_id === req.user.id
      || session.is_public;

    if (!hasAccess) {
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

// GET /sessions/:id/datahub
router.get(
  '/:id/datahub',
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

      // Convert to DataHub format
      const tracks = session.session_tracks.map((st) => {
        const { track } = st;
        const dataset = track.dataset_file?.dataset;

        // Determine file type and URL
        let fileType = 'unknown';
        let url = '';

        if (track.file_type === 'bam') {
          fileType = 'bam';
          url = `${process.env.API_BASE_URL}/files/${track.dataset_file_id}`;
        } else if (track.file_type === 'bigwig') {
          fileType = 'bigwig';
          url = `${process.env.API_BASE_URL}/files/${track.dataset_file_id}`;
        } else if (track.file_type === 'vcf') {
          fileType = 'vcf';
          url = `${process.env.API_BASE_URL}/files/${track.dataset_file_id}`;
        }

        return {
          name: track.name,
          type: fileType,
          url,
          color: st.color || '#000000',
          height: 50,
          genome: `${track.genomeType}_${track.genomeValue}`,
          dataset: dataset?.name || 'Unknown',
        };
      });

      res.json(tracks);
    } catch (error) {
      console.error('Error exporting DataHub:', error);
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
      const session = await prisma.genome_browser_session.findFirst({
        where: {
          id,
          OR: [
            { user_id: req.user.id }, // User's own sessions
            { is_public: true }, // Public sessions
          ],
        },
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
        projects = projects.filter((project) => project.name.toLowerCase().includes(search.toLowerCase())
          || (project.description && project.description.toLowerCase().includes(search.toLowerCase())));
      }

      // Apply sorting
      projects.sort((a, b) => {
        const aVal = a[sort_by];
        const bVal = b[sort_by];

        if (sort_order === 'asc') {
          return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
        }
        return aVal > bVal ? -1 : aVal < bVal ? 1 : 0;
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

module.exports = router;

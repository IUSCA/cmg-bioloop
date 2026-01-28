const { CMD_LINE_PROGRAMS, CONVERSION_DEFINITIONS, ARGUMENT_DATA, OTHER_PROGRAM_NAMES } = require('../constants');
const logger = require('../../logger');

/**
 * Seed constants: roles, cmd_line_programs, conversion_definitions, arguments
 * Equivalent to Python's create_roles() and _populate_pipeline_definitions()
 */

/**
 * Create roles in Bioloop database
 */
async function createRoles(prisma) {
  logger.info('[BIGBANG] Creating roles...');
  
  const bioloopRoles = [
    { name: 'admin', description: 'Access to the Admin Panel' },
    { name: 'operator', description: 'Operator level access' },
    { name: 'user', description: 'User level access' },
  ];

  let createdCount = 0;
  for (const role of bioloopRoles) {
    const existing = await prisma.role.findFirst({ where: { name: role.name } });
    if (!existing) {
      await prisma.role.create({
        data: {
          name: role.name,
          description: role.description,
        },
      });
      createdCount++;
    }
  }

  if (createdCount > 0) {
    logger.info(`[BIGBANG] Inserted ${createdCount} roles`);
  } else {
    logger.info('[BIGBANG] All roles already exist');
  }
}

/**
 * Create CMG system user (cmguser)
 */
async function createCMGUser(prisma) {
  logger.info('[BIGBANG] Creating CMG system user...');
  
  const cmguser = {
    username: 'cmguser',
    name: 'CMG User',
    email: 'cmguser@sca.iu.edu',
    cas_id: 'cmguser',
    _id: 'cmguser', // Special ID for system user
    active: true,
    roles: ['admin'],
  };

  // Check if user already exists
  let user = await prisma.user.findUnique({ where: { username: cmguser.username } });
  
  if (!user) {
    user = await prisma.user.create({
      data: {
        username: cmguser.username,
        email: cmguser.email,
        name: cmguser.name,
        cas_id: cmguser.cas_id,
        is_deleted: false,
        cmg_id: cmguser._id,
        created_at: new Date(),
      },
    });

    // Assign admin role
    const adminRole = await prisma.role.findFirst({ where: { name: 'admin' } });
    await prisma.user_role.create({
      data: {
        user_id: user.id,
        role_id: adminRole.id,
      },
    });

    logger.info(`[BIGBANG] Created CMG system user: ${cmguser.username}`);
  } else {
    logger.info(`[BIGBANG] CMG system user already exists: ${cmguser.username}`);
  }
  
  return user.id;
}

/**
 * Populate pipeline definitions: cmd_line_programs, conversion_definitions, arguments
 */
async function populatePipelineDefinitions(prisma, cmgUserId) {
  logger.info('[BIGBANG] Populating pipeline definitions...');

  // 1. Create cmd_line_programs
  // These paths are from CMG's production configuration at /N/project/CMG-SCA/...
  // Note: CMG stored default args in the executable path (e.g., "bcl2fastq -r 4 -w 4 -p 14")
  // In Bioloop, we separate the executable from args - default args can be added as argument defaults
  logger.info('[BIGBANG] Inserting cmd_line_programs...');
  let programsCreated = 0;
  let programsUpdated = 0;
  for (const program of CMD_LINE_PROGRAMS) {
    const existing = await prisma.cmd_line_program.findFirst({ where: { name: program.name } });
    if (!existing) {
      await prisma.cmd_line_program.create({
        data: {
          name: program.name,
          executable_path: program.executable_path,
          executable_directory: program.executable_directory || null,
          allow_additional_args: program.allow_additional_args,
        },
      });
      programsCreated++;
    } else {
      // Update if executable_path or executable_directory differs
      const needsUpdate = 
        existing.executable_path !== program.executable_path ||
        existing.executable_directory !== (program.executable_directory || null) ||
        existing.allow_additional_args !== program.allow_additional_args;
      
      if (needsUpdate) {
        await prisma.cmd_line_program.update({
          where: { id: existing.id },
          data: {
            executable_path: program.executable_path,
            executable_directory: program.executable_directory || null,
            allow_additional_args: program.allow_additional_args,
          },
        });
        programsUpdated++;
        logger.info(`[BIGBANG] Updated cmd_line_program: ${program.name}`);
      }
    }
  }
  logger.info(`[BIGBANG] Inserted ${programsCreated} cmd_line_programs, updated ${programsUpdated} (${CMD_LINE_PROGRAMS.length - programsCreated - programsUpdated} unchanged)`);

  // 2. Get program name to ID mapping
  const programs = await prisma.cmd_line_program.findMany();
  const programMap = {};
  for (const program of programs) {
    programMap[program.name] = program.id;
  }

  // 3. Create conversion_definitions
  logger.info('[BIGBANG] Inserting conversion_definitions...');
  let definitionsCreated = 0;
  let definitionsUpdated = 0;
  for (const definition of CONVERSION_DEFINITIONS) {
    const existing = await prisma.conversion_definition.findFirst({ where: { name: definition.name } });
    if (!existing) {
      await prisma.conversion_definition.create({
        data: {
          name: definition.name,
          description: definition.description,
          enabled: definition.enabled,
          dataset_types: definition.dataset_types,
          tags: definition.tags,
          capture_logs: definition.capture_logs,
          output_directory: definition.output_directory,
          program_id: programMap[definition.name],
          author_id: cmgUserId,
        },
      });
      definitionsCreated++;
    } else {
      // Update if output_directory or other critical fields differ
      const needsUpdate = 
        existing.output_directory !== definition.output_directory ||
        existing.description !== definition.description ||
        existing.enabled !== definition.enabled ||
        existing.capture_logs !== definition.capture_logs;
      
      if (needsUpdate) {
        await prisma.conversion_definition.update({
          where: { id: existing.id },
          data: {
            description: definition.description,
            enabled: definition.enabled,
            capture_logs: definition.capture_logs,
            output_directory: definition.output_directory,
            // Don't update dataset_types, tags, program_id as they shouldn't change
          },
        });
        definitionsUpdated++;
        logger.info(`[BIGBANG] Updated conversion_definition: ${definition.name}`);
      }
    }
  }
  logger.info(`[BIGBANG] Inserted ${definitionsCreated} conversion_definitions, updated ${definitionsUpdated} (${CONVERSION_DEFINITIONS.length - definitionsCreated - definitionsUpdated} unchanged)`);

  // 4. Create arguments
  logger.info('[BIGBANG] Inserting arguments...');
  const argumentDataWithPrograms = [];

  // bcl2fastq links to all args
  const bcl2fastqProgramId = programMap['bcl2fastq'];
  for (const arg of ARGUMENT_DATA) {
    argumentDataWithPrograms.push({
      ...arg,
      program_id: bcl2fastqProgramId,
    });
  }

  // Other programs link to specific shared args (including sample-sheet for CMG conversions)
  const sharedArgNames = ['--no-lane-splitting', '--delete-undetermined', '--filter-single-index', '--sample-sheet'];
  const conversionProgramsSharedArgs = ARGUMENT_DATA.filter(arg => sharedArgNames.includes(arg.name));

  for (const programName of OTHER_PROGRAM_NAMES) {
    const programId = programMap[programName];
    if (programId) {
      for (const arg of conversionProgramsSharedArgs) {
        argumentDataWithPrograms.push({
          ...arg,
          program_id: programId,
        });
      }
    }
  }

  // Insert all arguments
  let argumentsCreated = 0;
  for (const arg of argumentDataWithPrograms) {
    const existing = await prisma.argument.findFirst({
      where: {
        name: arg.name,
        program_id: arg.program_id,
      },
    });
    if (!existing) {
      await prisma.argument.create({
        data: {
          name: arg.name,
          value_type: arg.value_type,
          allowed_values: arg.allowed_values,
        is_required: arg.is_required,
        default_value: arg.default_value,
        is_flag: arg.is_flag,
        description: arg.description,
        min_value: arg.min_value,
        max_value: arg.max_value,
        min_length: arg.min_length,
        max_length: arg.max_length,
        position: arg.position,
        dynamic_variable_name: arg.dynamic_variable_name,
        program_id: arg.program_id,
      },
    });
      argumentsCreated++;
    }
  }
  logger.info(`[BIGBANG] Inserted ${argumentsCreated} arguments (${argumentDataWithPrograms.length - argumentsCreated} already existed)`);

  logger.info('[BIGBANG] Pipeline definitions populated successfully');
}

module.exports = {
  createRoles,
  createCMGUser,
  populatePipelineDefinitions,
};


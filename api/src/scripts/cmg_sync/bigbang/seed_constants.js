const { CMD_LINE_PROGRAMS, CONVERSION_DEFINITIONS, ARGUMENT_DATA, OTHER_PROGRAM_NAMES } = require('../constants');
const logger = require('@/services/logger');

/**
 * Seed constants: roles, cmd_line_programs, conversion_definitions, arguments
 * Equivalent to Python's create_roles() and _populate_pipeline_definitions()
 */

/**
 * Create roles in Bioloop database
 * Equivalent to: db_conversion/src/convert/entity/user.py::create_roles()
 */
async function createRoles(prisma) {
  logger.info('[BIGBANG] Creating roles...');
  
  const bioloopRoles = [
    { name: 'admin', description: 'Access to the Admin Panel' },
    { name: 'operator', description: 'Operator level access' },
    { name: 'user', description: 'User level access' },
  ];

  for (const role of bioloopRoles) {
    await prisma.role.create({
      data: {
        name: role.name,
        description: role.description,
      },
    });
  }

  logger.info(`[BIGBANG] Inserted ${bioloopRoles.length} roles`);
}

/**
 * Create CMG system user (cmguser)
 * Equivalent to: db_conversion/src/convert/constants/cmg.py::cmguser
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
    roles: ['user'],
  };

  const user = await prisma.user.create({
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

  // Assign user role
  const userRole = await prisma.role.findUnique({ where: { name: 'user' } });
  await prisma.user_role.create({
    data: {
      user_id: user.id,
      role_id: userRole.id,
    },
  });

  logger.info(`[BIGBANG] Created CMG system user: ${cmguser.username}`);
  return user.id;
}

/**
 * Populate pipeline definitions: cmd_line_programs, conversion_definitions, arguments
 * Equivalent to: db_conversion/src/convert/entity/conversion.py::_populate_pipeline_definitions()
 */
async function populatePipelineDefinitions(prisma, cmgUserId) {
  logger.info('[BIGBANG] Populating pipeline definitions...');

  // 1. Create cmd_line_programs
  logger.info('[BIGBANG] Inserting cmd_line_programs...');
  for (const program of CMD_LINE_PROGRAMS) {
    await prisma.cmd_line_program.create({
      data: {
        name: program.name,
        executable_path: program.executable_path,
        executable_directory: program.executable_directory,
        allow_additional_args: program.allow_additional_args,
      },
    });
  }

  // 2. Get program name to ID mapping
  const programs = await prisma.cmd_line_program.findMany();
  const programMap = {};
  for (const program of programs) {
    programMap[program.name] = program.id;
  }

  // 3. Create conversion_definitions
  logger.info('[BIGBANG] Inserting conversion_definitions...');
  for (const definition of CONVERSION_DEFINITIONS) {
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
  }

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

  // Other programs link to specific shared args
  const sharedArgNames = ['--no-lane-splitting', '--delete-undetermined', '--filter-single-index'];
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
  for (const arg of argumentDataWithPrograms) {
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
  }

  logger.info('[BIGBANG] Pipeline definitions populated successfully');
}

module.exports = {
  createRoles,
  createCMGUser,
  populatePipelineDefinitions,
};


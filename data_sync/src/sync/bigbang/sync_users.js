const logger = require('../../logger');
const { mapCMGRolesToBioloop } = require('../utils/role_mapper');

/**
 * Assign roles to a user based on CMG roles
 */
async function assignUserRoles(prisma, cmgUser, userId) {
  const cmgRoles = cmgUser.roles || [];
  const bioloopRoleNames = mapCMGRolesToBioloop(cmgRoles);

  // Get all Bioloop roles
  const bioloopRoles = await prisma.role.findMany({
    where: {
      name: { in: bioloopRoleNames },
    },
  });

  // Create user_role associations
  // eslint-disable-next-line no-restricted-syntax
  for (const role of bioloopRoles) {
    // eslint-disable-next-line no-await-in-loop
    await prisma.user_role.create({
      data: {
        user_id: userId,
        role_id: role.id,
      },
    });
  }
}

/**
 * Convert a single CMG user to Bioloop
 *
 * Returns: user object on success, null if user is a duplicate (skipped)
 * Throws: on any unexpected error
 */
async function convertUser(prisma, cmgUser) {
  // Check if user already exists by cas_id (username)
  // This prevents CMG migration from overwriting Bioloop users populated from JSON files
  const existingUser = await prisma.user.findFirst({
    where: { cas_id: cmgUser.username },
  });
  
  if (existingUser) {
    logger.warn(`[BIGBANG] Skipping CMG user (already exists as Bioloop user): ${cmgUser.username} - ${cmgUser.fullname}`);
    return null;
  }
  
  // Insert user
  let user;
  try {
    user = await prisma.user.create({
      data: {
        username: cmgUser.username,
        email: cmgUser.email,
        name: cmgUser.fullname || null,
        cas_id: cmgUser.username, // CMG uses username as cas_id
        is_deleted: !cmgUser.active,
        cmg_id: cmgUser._id.toString(),
        created_at: cmgUser.createDate || new Date(), // CMG uses 'createDate' (not createdDate)
      },
    });
  } catch (error) {
    // Only catch unique constraint violations - this is expected for duplicates
    if (error.code === 'P2002') {
      logger.warn(`[BIGBANG] Skipping duplicate user: ${cmgUser.email} - ${cmgUser.fullname}`);
      return null;
    }
    // Any other error should propagate up and stop the migration
    throw error;
  }

  // Assign roles - let any errors propagate
  await assignUserRoles(prisma, cmgUser, user.id);

  return user;
}

/**
 * Convert CMG users to Bioloop users
 */
async function syncUsers(prisma, cmgDb) {
  logger.info('[BIGBANG] Converting users...');

  const cmgUsers = await cmgDb.collection('users').find({}).toArray();
  logger.info(`[BIGBANG] Found ${cmgUsers.length} CMG users to convert`);

  let convertedCount = 0;
  let skippedCount = 0;

  // eslint-disable-next-line no-restricted-syntax
  for (const cmgUser of cmgUsers) {
    // eslint-disable-next-line no-await-in-loop
    const result = await convertUser(prisma, cmgUser);
    if (result) {
      convertedCount += 1;
    } else {
      skippedCount += 1;
    }
  }

  logger.info(`[BIGBANG] User conversion complete: ${convertedCount} succeeded, ${skippedCount} skipped (duplicates)`);
}

/**
 * Get Bioloop CMG system user ID
 */
async function getBioloopCMGUserId(prisma) {
  const user = await prisma.user.findUnique({
    where: { username: 'cmguser' },
  });

  if (!user) {
    throw new Error('User "cmguser" not found in the PostgreSQL database');
  }

  return user.id;
}

module.exports = {
  syncUsers,
  convertUser,
  assignUserRoles,
  getBioloopCMGUserId,
};

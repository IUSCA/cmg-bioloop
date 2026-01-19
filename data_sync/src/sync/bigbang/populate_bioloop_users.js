const fs = require('fs');
const path = require('path');
const logger = require('../../logger');

/**
 * Read users from JSON file (similar to api/src/utils/index.js)
 */
function readUsersFromJSON(filePath) {
  try {
    const exists = fs.existsSync(filePath);
    
    if (!exists) {
      logger.warn(`[BIOLOOP USERS] File not found: ${filePath}`);
      return [];
    }
    
    const content = fs.readFileSync(filePath, 'utf8');
    const users = JSON.parse(content);
    
    if (!Array.isArray(users)) {
      logger.warn(`[BIOLOOP USERS] Invalid format in ${filePath} - expected array`);
      return [];
    }
    
    return users;
  } catch (error) {
    logger.error(`[BIOLOOP USERS] Error reading ${filePath}: ${error.message}`);
    return [];
  }
}

/**
 * Get role ID by name
 */
async function getRoleId(prisma, roleName) {
  const role = await prisma.role.findFirst({
    where: { name: roleName },
  });
  
  if (!role) {
    throw new Error(`Role "${roleName}" not found in database`);
  }
  
  return role.id;
}

/**
 * Populate Bioloop users from JSON files before CMG migration
 * 
 * This ensures that production Bioloop users (admins, operators, regular users)
 * are created first, so CMG migration can skip them if they already exist.
 */
async function populateBioloopUsers(prisma) {
  logger.info('[BIOLOOP USERS] Populating Bioloop users from JSON files...');
  
  // Resolve path to api directory (go up from data_sync to project root, then to api)
  const apiDir = path.resolve(__dirname, '../../../../api');
  
  // Define user categories and their corresponding JSON files and roles
  const userCategories = [
    {
      name: 'admins',
      file: path.join(apiDir, 'admins.json'),
      roleId: await getRoleId(prisma, 'admin'),
    },
    {
      name: 'operators',
      file: path.join(apiDir, 'operators.json'),
      roleId: await getRoleId(prisma, 'operator'),
    },
    {
      name: 'users',
      file: path.join(apiDir, 'users.json'),
      roleId: await getRoleId(prisma, 'user'),
    },
  ];
  
  // Hardcoded service account
  const serviceAccounts = [
    {
      name: 'svc_tasks',
      username: 'svc_tasks',
      email: 'svc_tasks@iu.edu',
      roleId: await getRoleId(prisma, 'admin'),
    },
  ];
  
  let totalCreated = 0;
  let totalSkipped = 0;
  
  // Create service accounts first
  for (const account of serviceAccounts) {
    try {
      const existingUser = await prisma.user.findFirst({
        where: { cas_id: account.username },
      });
      
      if (existingUser) {
        logger.info(`[BIOLOOP USERS] Service account already exists: ${account.username}`);
        totalSkipped += 1;
        continue;
      }
      
      const user = await prisma.user.create({
        data: {
          username: account.username,
          email: account.email,
          name: account.name,
          cas_id: account.username,
          is_deleted: false,
        },
      });
      
      await prisma.user_role.create({
        data: {
          user_id: user.id,
          role_id: account.roleId,
        },
      });
      
      logger.info(`[BIOLOOP USERS] Created service account: ${account.username}`);
      totalCreated += 1;
    } catch (error) {
      if (error.code === 'P2002') {
        logger.warn(`[BIOLOOP USERS] Duplicate service account: ${account.username}`);
        totalSkipped += 1;
      } else {
        throw error;
      }
    }
  }
  
  // Process each category (admins, operators, users)
  for (const category of userCategories) {
    const users = readUsersFromJSON(category.file);
    
    if (users.length === 0) {
      logger.warn(`[BIOLOOP USERS] No ${category.name} found in ${category.file}`);
      continue;
    }
    
    logger.info(`[BIOLOOP USERS] Processing ${users.length} ${category.name} from ${path.basename(category.file)}`);
    
    for (const userRecord of users) {
      try {
        // Check if user already exists by cas_id (which maps to username)
        const existingUser = await prisma.user.findFirst({
          where: { cas_id: userRecord.username },
        });
        
        if (existingUser) {
          logger.info(`[BIOLOOP USERS] User already exists: ${userRecord.username} (${category.name})`);
          totalSkipped += 1;
          continue;
        }
        
        // Create user
        const user = await prisma.user.create({
          data: {
            username: userRecord.username,
            email: userRecord.email,
            name: userRecord.name,
            cas_id: userRecord.username, // Same pattern as init_prod_users.js
            is_deleted: false,
          },
        });
        
        // Assign role
        await prisma.user_role.create({
          data: {
            user_id: user.id,
            role_id: category.roleId,
          },
        });
        
        logger.info(`[BIOLOOP USERS] Created ${category.name}: ${userRecord.username}`);
        totalCreated += 1;
      } catch (error) {
        if (error.code === 'P2002') {
          logger.warn(`[BIOLOOP USERS] Duplicate user: ${userRecord.username} (${category.name})`);
          totalSkipped += 1;
        } else {
          logger.error(`[BIOLOOP USERS] Error creating user ${userRecord.username}: ${error.message}`);
          throw error;
        }
      }
    }
  }
  
  logger.info(`[BIOLOOP USERS] Bioloop user population complete: ${totalCreated} created, ${totalSkipped} skipped`);
  
  return { created: totalCreated, skipped: totalSkipped };
}

module.exports = {
  populateBioloopUsers,
  readUsersFromJSON,
};


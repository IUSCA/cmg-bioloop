const fs = require('fs');
const path = require('path');

const ROLE_SEED = [
  { id: 1, name: 'admin', description: 'Access to the Admin Panel' },
  { id: 2, name: 'operator', description: 'Operator level access' },
  { id: 3, name: 'user', description: 'User level access' },
];

const API_JSON_DIR_CANDIDATES = [
  '/opt/sca/api',
  '/opt/sca/cmg/api',
  path.resolve(__dirname, '../../../../api'),
];

function readUsersFromJson(fileName) {
  for (const baseDir of API_JSON_DIR_CANDIDATES) {
    const filePath = path.join(baseDir, fileName);
    if (!fs.existsSync(filePath)) continue;

    const raw = fs.readFileSync(filePath, 'utf8');
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      throw new Error(`Expected array in ${filePath}`);
    }
    return parsed;
  }

  throw new Error(
    `Could not locate ${fileName}. Tried: ${API_JSON_DIR_CANDIDATES.join(', ')}`,
  );
}

function withRole(users, roleId) {
  return users.map((user) => ({
    ...user,
    cas_id: user.username,
    role_id: roleId,
  }));
}

async function updateSeq(prisma, table) {
  const result = await prisma[table].aggregate({
    _max: { id: true },
  });
  const currentMaxId = result?._max?.id || 0;
  await prisma.$executeRawUnsafe(`ALTER SEQUENCE ${table}_id_seq RESTART WITH ${currentMaxId + 1}`);
}

async function bootstrapProdUsers(prisma, logger, label = 'BIGBANG') {
  logger.info(`[${label}] Bootstrapping production roles/users from api JSON files...`);

  await Promise.allSettled(ROLE_SEED.map((role) => prisma.role.upsert({
    where: { id: role.id },
    create: role,
    update: role,
  })));

  const staticAdmins = [{
    name: 'svc_tasks',
    username: 'svc_tasks',
    email: 'svc_tasks@iu.edu',
  }];

  const admins = withRole(staticAdmins.concat(readUsersFromJson('admins.json')), 1);
  const operators = withRole(readUsersFromJson('operators.json'), 2);
  const users = withRole(readUsersFromJson('users.json'), 3);
  const allUsers = admins.concat(operators, users);

  const seenUsernames = new Set();
  const seenEmails = new Set();
  const dedupedUsers = allUsers.filter((user) => {
    if (seenUsernames.has(user.username) || seenEmails.has(user.email)) return false;
    seenUsernames.add(user.username);
    seenEmails.add(user.email);
    return true;
  });

  for (const user of dedupedUsers) {
    // eslint-disable-next-line no-await-in-loop
    const existing = await prisma.user.findFirst({
      where: {
        OR: [
          { username: user.username },
          { email: user.email },
        ],
      },
      select: { id: true },
    });

    let userId;
    if (existing) {
      userId = existing.id;
    } else {
      // eslint-disable-next-line no-await-in-loop
      const created = await prisma.user.create({
        data: {
          username: user.username,
          name: user.name || null,
          email: user.email,
          cas_id: user.cas_id || user.username,
        },
        select: { id: true },
      });
      userId = created.id;
    }

    // eslint-disable-next-line no-await-in-loop
    await prisma.user_role.upsert({
      where: {
        user_id_role_id: {
          user_id: userId,
          role_id: user.role_id,
        },
      },
      create: {
        user_id: userId,
        role_id: user.role_id,
      },
      update: {},
    });
  }

  await Promise.all([updateSeq(prisma, 'user'), updateSeq(prisma, 'role')]);

  logger.info(
    `[${label}] Production user bootstrap complete `
    + `(${admins.length} admins, ${operators.length} operators, ${users.length} users)`,
  );
}

module.exports = { bootstrapProdUsers };

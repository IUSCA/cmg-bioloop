const { MongoClient, ObjectId } = require('mongodb');
const { PrismaClient } = require('@prisma/client');

(async () => {
  const cmgClient = await MongoClient.connect('mongodb://db:27017/cmg');
  const cmgDb = cmgClient.db('cmg');
  
  const cmgUser = await cmgDb.collection('users').findOne(
    { _id: new ObjectId('6349af6fbaadac06f1bf709f') }
  );
  
  console.log('=== CMG User (6349af6fbaadac06f1bf709f) ===');
  console.log('username:', cmgUser.username);
  console.log('roles:', cmgUser.roles);
  console.log('updatedAt:', cmgUser.updatedAt);
  
  await cmgClient.close();
  
  const prisma = new PrismaClient({
    datasources: { db: { url: 'postgresql://appuser:example@postgres:5432/app' } }
  });
  
  const bioloopUser = await prisma.user.findFirst({
    where: { cmg_id: '6349af6fbaadac06f1bf709f' },
    include: { user_role: { include: { roles: true } } }
  });
  
  console.log('\n=== Bioloop User ===');
  console.log('username:', bioloopUser.username);
  console.log('roles:', bioloopUser.user_role.map(ur => ur.roles.name));
  console.log('updated_at:', bioloopUser.updated_at.toISOString());
  
  console.log('\n=== Comparison ===');
  console.log('Expected role: operator');
  const hasOperator = bioloopUser.user_role.some(ur => ur.roles.name === 'operator');
  console.log('Has operator role?', hasOperator ? '✅ YES' : '❌ NO');
  
  if (hasOperator) {
    const cmgTime = new Date(cmgUser.updatedAt);
    const bioloopTime = new Date(bioloopUser.updated_at);
    const delayMs = bioloopTime - cmgTime;
    console.log('\n✅ SYNC SUCCESSFUL!');
    console.log('Sync delay:', delayMs + 'ms (' + (delayMs/1000).toFixed(1) + 's)');
  } else {
    console.log('\n❌ NOT SYNCED YET or FAILED');
  }
  
  await prisma.$disconnect();
})();

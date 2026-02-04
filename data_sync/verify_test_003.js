const { MongoClient, ObjectId } = require('mongodb');
const { PrismaClient } = require('@prisma/client');

(async () => {
  const cmgClient = await MongoClient.connect('mongodb://db:27017/cmg');
  const cmgDb = cmgClient.db('cmg');
  
  const cmgDataset = await cmgDb.collection('dataproducts').findOne(
    { _id: new ObjectId('697d210dc6f82169bc85f884') }
  );
  
  console.log('CMG Dataset 697d210dc6f82169bc85f884:');
  console.log('  description:', cmgDataset.description);
  console.log('  updatedAt:', cmgDataset.updatedAt);
  
  await cmgClient.close();
  
  const prisma = new PrismaClient({
    datasources: { db: { url: 'postgresql://appuser:example@postgres:5432/app' } }
  });
  
  const bioloopDataset = await prisma.dataset.findFirst({
    where: { cmg_id: '697d210dc6f82169bc85f884' }
  });
  
  console.log('\nBioloop Dataset:');
  console.log('  description:', bioloopDataset.description);
  console.log('  updated_at:', bioloopDataset.updated_at.toISOString());
  
  console.log('\nDo they match?', cmgDataset.description === bioloopDataset.description ? '✅ YES' : '❌ NO');
  
  if (cmgDataset.description === bioloopDataset.description) {
    console.log('\ntest_003 dataset description: ACTUALLY SYNCED ✅');
  } else {
    console.log('\ntest_003 dataset description: NOT SYNCED ❌');
  }
  
  await prisma.$disconnect();
})();

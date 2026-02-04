const { MongoClient, ObjectId } = require('mongodb');
const { PrismaClient } = require('@prisma/client');

(async () => {
  // Check CMG
  const cmgClient = await MongoClient.connect('mongodb://db:27017/cmg');
  const cmgDb = cmgClient.db('cmg');
  const cmgDataset = await cmgDb.collection('dataproducts').findOne(
    { _id: new ObjectId('5fbd7266c64cc935805cc9d3') },
    { projection: { files: 1, updatedAt: 1, name: 1 } }
  );
  
  console.log('=== CMG MongoDB ===');
  console.log('Dataset:', cmgDataset.name);
  console.log('files:', cmgDataset.files);
  console.log('updatedAt:', cmgDataset.updatedAt);
  
  await cmgClient.close();
  
  // Check Bioloop
  const prisma = new PrismaClient({
    datasources: { db: { url: 'postgresql://appuser:example@postgres:5432/app' } }
  });
  
  const bioloopDataset = await prisma.dataset.findFirst({
    where: { cmg_id: '5fbd7266c64cc935805cc9d3' },
    select: { num_files: true, updated_at: true, name: true }
  });
  
  console.log('\n=== Bioloop PostgreSQL ===');
  console.log('Dataset:', bioloopDataset.name);
  console.log('num_files:', bioloopDataset.num_files);
  console.log('updated_at:', bioloopDataset.updated_at.toISOString());
  
  console.log('\n=== Comparison ===');
  if (cmgDataset.files === bioloopDataset.num_files) {
    console.log('✅ VALUES MATCH! test_006 synced successfully!');
    const cmgTime = new Date(cmgDataset.updatedAt);
    const bioloopTime = new Date(bioloopDataset.updated_at);
    const delayMs = bioloopTime - cmgTime;
    console.log('Sync delay:', delayMs + 'ms (' + (delayMs/1000).toFixed(1) + 's)');
  } else {
    console.log('❌ VALUES DO NOT MATCH');
    console.log('Expected (CMG):', cmgDataset.files);
    console.log('Actual (Bioloop):', bioloopDataset.num_files);
  }
  
  await prisma.$disconnect();
})();

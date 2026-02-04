const { PrismaClient } = require('@prisma/client');

(async () => {
  const prisma = new PrismaClient({
    datasources: { db: { url: 'postgresql://appuser:example@postgres:5432/app' } }
  });
  
  const projectCount = await prisma.project.count();
  const userCount = await prisma.user.count();
  const datasetCount = await prisma.dataset.count();
  
  console.log('📊 Database counts after bigbang:');
  console.log(`  Projects: ${projectCount}`);
  console.log(`  Users: ${userCount}`);
  console.log(`  Datasets: ${datasetCount}\n`);
  
  // Find test records
  const testProject = await prisma.project.findFirst({
    where: { cmg_id: '606b521f02d8137b0ff40049' },
    select: { cmg_id: true, name: true, description: true }
  });
  
  const sampleDataset = await prisma.dataset.findFirst({
    where: { cmg_id: { not: null } },
    select: { cmg_id: true, name: true, type: true, description: true },
    orderBy: { created_at: 'desc' }
  });
  
  console.log('🎯 Test records available:');
  if (testProject) {
    console.log(`  ✅ Project: ${testProject.cmg_id} - ${testProject.name}`);
  } else {
    console.log(`  ❌ Project 606b521f02d8137b0ff40049 NOT FOUND`);
  }
  
  if (sampleDataset) {
    console.log(`  ✅ Dataset: ${sampleDataset.cmg_id} - ${sampleDataset.name} (${sampleDataset.type})`);
  } else {
    console.log(`  ❌ No datasets found`);
  }
  
  await prisma.$disconnect();
})();

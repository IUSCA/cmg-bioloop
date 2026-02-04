const { PrismaClient } = require('@prisma/client');

(async () => {
  const prisma = new PrismaClient({
    datasources: { db: { url: 'postgresql://appuser:example@postgres:5432/app' } }
  });
  
  console.log('=== BIOLOOP VALUES ===\n');
  
  const user = await prisma.user.findFirst({
    where: { cmg_id: '5f577fb638972540c2718122' },
    select: { name: true, email: true, updated_at: true }
  });
  
  console.log('User (5f577fb638972540c2718122):');
  console.log(`  name: ${user.name}`);
  console.log(`  email: ${user.email}`);
  console.log(`  updated_at: ${user.updated_at.toISOString()}\n`);
  
  const project = await prisma.project.findFirst({
    where: { cmg_id: '606b521f02d8137b0ff40049' },
    select: { description: true, updated_at: true }
  });
  
  console.log('Project (606b521f02d8137b0ff40049):');
  console.log(`  description: ${project.description}`);
  console.log(`  updated_at: ${project.updated_at.toISOString()}`);
  
  await prisma.$disconnect();
})();

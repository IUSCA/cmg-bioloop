const { PrismaClient } = require('@prisma/client');

(async () => {
  const prisma = new PrismaClient({
    datasources: { db: { url: 'postgresql://appuser:example@postgres:5432/app' } }
  });
  
  const dataset = await prisma.dataset.findFirst({
    where: { cmg_id: '697d210dc6f82169bc85f884' },
    select: { description: true, size: true, is_staged: true, updated_at: true }
  });
  
  console.log('Bioloop dataset 697d210dc6f82169bc85f884:');
  if (dataset) {
    console.log('  description:', dataset.description);
    console.log('  size:', dataset.size.toString());
    console.log('  is_staged:', dataset.is_staged);
    console.log('  updated_at:', dataset.updated_at.toISOString());
  } else {
    console.log('  NOT FOUND');
  }
  
  await prisma.$disconnect();
})();

/* eslint-disable no-console */
require('module-alias/register');
const path = require('path');
const { PrismaClient } = require('@prisma/client');

global.__basedir = path.join(__dirname, '..', '..');

const prisma = new PrismaClient();

const importSources = [
  {
    path: '/N/project/yunliu-general/SCA_incoming',
    label: 'SCA Incoming',
    description: 'SCA incoming data on Slate-Project filesystem',
    sort_order: 1,
  },
  {
    path: '/N/project/CMG-SCA',
    label: 'CMG-SCA',
    description: 'CMG-SCA incoming data on Slate-Project filesystem',
    sort_order: 2,
  },
  {
    path: '/N/scratch/cmguser/cmg-bioloop/imports',
    label: 'CMG-Bioloop Slate-Scratch',
    description: 'Incoming data on CMG-Bioloop Slate-Scratch filesystem',
    sort_order: 3,
  },
  {
    path: '/N/project/CMG-SCA/cmg-bioloop/imports',
    label: 'CMG-Bioloop Slate-Project',
    description: 'Incoming data on CMG-Bioloop Slate-Project filesystem',
    sort_order: 4,
  },
];

async function main() {
  await Promise.all(
    importSources.map((source) => prisma.import_source.upsert({
      where: { path: source.path },
      create: source,
      update: { label: source.label, description: source.description, sort_order: source.sort_order },
    })),
  );

  console.log(`created/updated ${importSources.length} import sources`);
}

main()
  .then(() => {
    prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });

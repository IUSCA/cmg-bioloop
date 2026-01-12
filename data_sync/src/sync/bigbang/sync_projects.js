const { ObjectId } = require('mongodb');
const { expandGroups, generateSlug } = require('../utils/cmg_helpers');
const logger = require('../logger');

/**
 * Convert projects from CMG to Bioloop
 * Equivalent to: db_conversion/src/convert/entity/project.py::convert_projects()
 */
async function syncProjects(prisma, cmgDb) {
  logger.info('[BIGBANG] Converting projects...');
  
  const cmgProjects = await cmgDb.collection('projects').find({}).toArray();
  logger.info(`[BIGBANG] Found ${cmgProjects.length} CMG projects to convert`);
  
  // Step 1: Batch insert all projects
  const projectData = [];
  for (const project of cmgProjects) {
    const slug = await generateSlug(prisma, project.name, project._id.toString());
    projectData.push({
      name: project.name,
      description: project.description || null,
      slug: slug,
      created_at: project.createdAt || new Date(),
      updated_at: project.updatedAt || new Date(),
      browser_enabled: project.browser || false,
      cmg_id: project._id.toString(),
    });
  }
  
  // Batch create all projects
  await prisma.project.createMany({
    data: projectData,
  });
  
  logger.info(`[BIGBANG] Inserted ${projectData.length} projects`);
  
  // Step 2: Create project associations (user and dataset)
  const projectUserAssociations = [];
  const projectDatasetAssociations = [];
  
  for (const cmgProject of cmgProjects) {
    // Find the Bioloop project we just created
    const bioloopProject = await prisma.project.findUnique({
      where: { cmg_id: cmgProject._id.toString() },
    });
    
    if (!bioloopProject) {
      logger.warn(`[BIGBANG] Bioloop project not found for CMG ID: ${cmgProject._id}`);
      continue;
    }
    
    // Get all users (direct + from groups)
    const directUsers = cmgProject.users || [];
    const groupUsers = await expandGroups(cmgDb, cmgProject.groups || []);
    const allUserIds = [...new Set([...directUsers.map(id => id.toString()), ...groupUsers])];
    
    // Find corresponding Bioloop users
    for (const cmgUserId of allUserIds) {
      const bioloopUser = await prisma.user.findUnique({
        where: { cmg_id: cmgUserId },
      });
      
      if (bioloopUser) {
        projectUserAssociations.push({
          project_id: bioloopProject.id,
          user_id: bioloopUser.id,
        });
      } else {
        logger.warn(`[BIGBANG] User not found for CMG ID: ${cmgUserId}`);
      }
    }
    
    // Get all dataproducts
    const dataproductIds = cmgProject.dataproducts || [];
    for (const cmgDataproductId of dataproductIds) {
      const bioloopDataset = await prisma.dataset.findUnique({
        where: { cmg_id: cmgDataproductId.toString() },
      });
      
      if (bioloopDataset) {
        projectDatasetAssociations.push({
          project_id: bioloopProject.id,
          dataset_id: bioloopDataset.id,
        });
      } else {
        logger.warn(`[BIGBANG] Dataset not found for CMG ID: ${cmgDataproductId}`);
      }
    }
  }
  
  // Batch insert associations
  if (projectUserAssociations.length > 0) {
    await prisma.project_user.createMany({
      data: projectUserAssociations,
      skipDuplicates: true,
    });
    logger.info(`[BIGBANG] Created ${projectUserAssociations.length} project-user associations`);
  }
  
  if (projectDatasetAssociations.length > 0) {
    await prisma.project_dataset.createMany({
      data: projectDatasetAssociations,
      skipDuplicates: true,
    });
    logger.info(`[BIGBANG] Created ${projectDatasetAssociations.length} project-dataset associations`);
  }
  
  logger.info('[BIGBANG] Project conversion complete');
}

module.exports = {
  syncProjects,
};


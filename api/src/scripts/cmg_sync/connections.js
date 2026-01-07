/**
 * CMG Sync - Database Connections
 * 
 * Manages MongoDB (CMG) and Prisma (Bioloop) connections
 */

const { MongoClient } = require('mongodb');
const config = require('config');
const prisma = require('@/db');
const logger = require('@/services/logger');

let cmgClient = null;
let cmgDb = null;

/**
 * Initialize MongoDB connection to CMG database
 */
async function initializeCMGConnection() {
  if (cmgClient && cmgDb) {
    logger.info('[CMG Sync] CMG MongoDB already connected');
    return { client: cmgClient, db: cmgDb };
  }

  const cmgConfig = config.get('cmg_mongodb');
  
  const uri = `mongodb://${cmgConfig.username}:${cmgConfig.password}@${cmgConfig.host}:${cmgConfig.port}/${cmgConfig.database}?authSource=${cmgConfig.authSource}`;
  
  logger.info('[CMG Sync] Connecting to CMG MongoDB...');
  logger.info(`[CMG Sync] Host: ${cmgConfig.host}:${cmgConfig.port}, Database: ${cmgConfig.database}`);
  
  cmgClient = new MongoClient(uri, {
    maxPoolSize: 10,
    minPoolSize: 2,
    serverSelectionTimeoutMS: 5000,
  });
  
  await cmgClient.connect();
  cmgDb = cmgClient.db(cmgConfig.database);
  
  // Test connection
  await cmgDb.command({ ping: 1 });
  logger.info('[CMG Sync] CMG MongoDB connected successfully');
  
  return { client: cmgClient, db: cmgDb };
}

/**
 * Get CMG MongoDB database instance
 */
function getCMGDatabase() {
  if (!cmgDb) {
    throw new Error('CMG MongoDB not initialized. Call initializeCMGConnection() first.');
  }
  return cmgDb;
}

/**
 * Get Prisma client instance
 */
function getPrismaClient() {
  return prisma;
}

/**
 * Close all database connections
 */
async function closeConnections() {
  logger.info('[CMG Sync] Closing database connections...');
  
  if (cmgClient) {
    await cmgClient.close();
    cmgClient = null;
    cmgDb = null;
    logger.info('[CMG Sync] CMG MongoDB connection closed');
  }
  
  await prisma.$disconnect();
  logger.info('[CMG Sync] Prisma connection closed');
}

/**
 * Test both connections
 */
async function testConnections() {
  try {
    // Test CMG MongoDB
    const { db } = await initializeCMGConnection();
    const collections = await db.listCollections().toArray();
    logger.info(`[CMG Sync] CMG has ${collections.length} collections`);
    
    // Test Prisma
    await prisma.$queryRaw`SELECT 1`;
    logger.info('[CMG Sync] Bioloop PostgreSQL connection OK');
    
    return true;
  } catch (error) {
    logger.error('[CMG Sync] Connection test failed:', error);
    throw error;
  }
}

module.exports = {
  initializeCMGConnection,
  getCMGDatabase,
  getPrismaClient,
  closeConnections,
  testConnections,
};


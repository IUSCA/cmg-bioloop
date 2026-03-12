const config = require('config');

function readLegacyConfig() {
  try {
    return config.get('legacy_application_active');
  } catch (error) {
    return true;
  }
}

function isLegacySourceActive(sourceName = 'xenium') {
  const legacyConfig = readLegacyConfig();

  if (typeof legacyConfig === 'boolean') return legacyConfig;
  if (legacyConfig && typeof legacyConfig === 'object') {
    if (Object.prototype.hasOwnProperty.call(legacyConfig, sourceName)) {
      return Boolean(legacyConfig[sourceName]);
    }
    if (Object.prototype.hasOwnProperty.call(legacyConfig, 'cmg')) {
      return Boolean(legacyConfig.cmg);
    }
  }

  return true;
}

function getDatasetOrigin(sourceName = 'xenium') {
  return isLegacySourceActive(sourceName) ? `legacy_${sourceName}` : 'bioloop';
}

function withDatasetOrigin(metadata, sourceName = 'xenium') {
  return {
    ...(metadata && typeof metadata === 'object' ? metadata : {}),
    origin: getDatasetOrigin(sourceName),
  };
}

function coerceIntegerId(value) {
  if (typeof value === 'number' && Number.isInteger(value)) return value;
  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = Number.parseInt(value, 10);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
}

function toDateOrNow(value) {
  const date = value ? new Date(value) : null;
  if (!date || Number.isNaN(date.getTime())) return new Date();
  return date;
}

function splitIntoChunks(values, chunkSize = 200) {
  if (!Array.isArray(values) || values.length === 0) return [];
  const chunks = [];
  for (let i = 0; i < values.length; i += chunkSize) {
    chunks.push(values.slice(i, i + chunkSize));
  }
  return chunks;
}

function slugify(value) {
  return String(value || '')
    .toLowerCase()
    .trim()
    .replace(/[\W_]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

async function generateUniqueProjectSlug(prisma, projectName, xeniumId) {
  const baseSlug = slugify(projectName) || `project-${xeniumId || Date.now()}`;
  let slug = baseSlug;
  let n = 1;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    // eslint-disable-next-line no-await-in-loop
    const existing = await prisma.project.findUnique({ where: { slug } });
    if (!existing || existing.xenium_id === xeniumId) {
      return slug;
    }
    slug = `${baseSlug}-${n}`;
    n += 1;
  }
}

module.exports = {
  isLegacySourceActive,
  getDatasetOrigin,
  withDatasetOrigin,
  coerceIntegerId,
  toDateOrNow,
  splitIntoChunks,
  generateUniqueProjectSlug,
};

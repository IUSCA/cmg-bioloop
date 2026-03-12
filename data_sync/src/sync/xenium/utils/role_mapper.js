/**
 * Xenium roles map to the same Bioloop role names.
 * Unknown roles are downgraded to "user".
 */
function mapXeniumRolesToBioloop(xeniumRoles) {
  const roles = Array.isArray(xeniumRoles) ? xeniumRoles : [];
  const mapped = new Set();

  for (const roleName of roles) {
    const normalized = String(roleName || '').toLowerCase().trim();
    if (normalized === 'admin') mapped.add('admin');
    else if (normalized === 'operator') mapped.add('operator');
    else mapped.add('user');
  }

  if (mapped.size === 0) {
    mapped.add('user');
  }

  return Array.from(mapped);
}

module.exports = { mapXeniumRolesToBioloop };

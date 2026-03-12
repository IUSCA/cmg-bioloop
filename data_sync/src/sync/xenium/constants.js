/**
 * Xenium Sync — Constants
 *
 * Constants specific to the Xenium → Bioloop migration.
 * Xenium is a bioloop fork; it has no Sessions, Conversions, or Tracks features.
 */

/**
 * Synthetic system user created in Bioloop to represent the Xenium application
 * for rows that have no natural owner (audit logs, import logs, etc.).
 *
 * Note: Xenium operations use the same service account user as CMG (`cmguser`).
 */
const XENIUM_USER = {
  username: 'cmguser',
  email: 'cmguser@system.local',
  fullname: 'CMG Service User',
  cas_id: 'cmguser',
  active: true,
  roles: ['admin'],
  createdDate: new Date(),
};

/**
 * Poller names for xenium sync cursors.
 * Must match the `poller_name` values stored in `xenium_sync_cursor`.
 */
const XENIUM_POLLER_NAMES = {
  USER_ROLES: 'xenium_user_roles',
  PROJECT_ACL: 'xenium_project_acl',
  DATASET_METADATA: 'xenium_dataset_metadata',
  PROJECT_METADATA: 'xenium_project_metadata',
};

/**
 * Import source paths for Xenium data registered during bigbang.
 * Adapt these to actual production paths as needed.
 */
const XENIUM_IMPORT_SOURCES = [
  {
    path: '/zpool/xenium',
    label: 'Xenium zpool',
    description: 'Xenium instrument output on the archive zpool',
    sort_order: 100,
  },
  {
    path: '/N/scratch/cmguser/xenium',
    label: 'Xenium Slate scratch',
    description: 'Xenium staged data on Slate scratch',
    sort_order: 101,
  },
];

module.exports = {
  XENIUM_USER,
  XENIUM_POLLER_NAMES,
  XENIUM_IMPORT_SOURCES,
};

#!/bin/bash
set -e

echo "Starting db_sandbox container..."

# Create log directory
mkdir -p /var/log/postgresql
chown postgres:postgres /var/log/postgresql

# Initialize PostgreSQL if needed
if [ ! -f "/var/lib/postgresql/15/main/postgresql.conf" ]; then
    echo "Initializing PostgreSQL..."
    mkdir -p /var/lib/postgresql/15/main
    chown -R postgres:postgres /var/lib/postgresql
    # Remove any leftover files from incomplete initialization
    rm -rf /var/lib/postgresql/15/main/*
    su - postgres -c "/usr/lib/postgresql/15/bin/initdb -D /var/lib/postgresql/15/main"
else
    echo "PostgreSQL already initialized"
fi

# Ensure proper ownership
echo "Setting ownership..."
chown -R postgres:postgres /var/lib/postgresql

# Start PostgreSQL
echo "Starting PostgreSQL..."
su - postgres -c "/usr/lib/postgresql/15/bin/pg_ctl -D /var/lib/postgresql/15/main -l /var/log/postgresql/postgresql.log start" || {
    echo "PostgreSQL failed to start. Checking log..."
    cat /var/log/postgresql/postgresql.log
    exit 1
}

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until su - postgres -c "psql -c '\q'" 2>/dev/null; do
    sleep 1
done

# Create database and user if they don't exist
echo "Setting up database..."
su - postgres -c "psql -c \"CREATE USER ${SYNC_PG_USER:-appuser} WITH PASSWORD '${SYNC_PG_PASSWORD:-example}'\"" 2>/dev/null || true
su - postgres -c "psql -c \"CREATE DATABASE ${SYNC_PG_DATABASE:-bioloop_sync}\"" 2>/dev/null || true
su - postgres -c "psql -c \"GRANT ALL PRIVILEGES ON DATABASE ${SYNC_PG_DATABASE:-bioloop_sync} TO ${SYNC_PG_USER:-appuser}\"" 2>/dev/null || true
su - postgres -c "psql -d ${SYNC_PG_DATABASE:-bioloop_sync} -c \"GRANT ALL ON SCHEMA public TO ${SYNC_PG_USER:-appuser}\"" 2>/dev/null || true
su - postgres -c "psql -d ${SYNC_PG_DATABASE:-bioloop_sync} -c \"GRANT CREATE ON SCHEMA public TO ${SYNC_PG_USER:-appuser}\"" 2>/dev/null || true

echo "PostgreSQL is ready!"

# Generate Prisma Client from main app's schema
echo "Generating Prisma Client from main app's schema..."
/opt/sca/app/node_modules/.bin/prisma generate --schema=/opt/sca/api/prisma/schema.prisma || echo "Warning: Prisma generate failed"

# Generate dedicated Prisma Client for Xenium source schema
echo "Generating Prisma Client for Xenium source schema..."
/opt/sca/app/node_modules/.bin/prisma generate --schema=/opt/sca/app/prisma/xenium_source.prisma || echo "Warning: Xenium Prisma generate failed"

# Run Prisma migrations from main app's migrations directory
echo "Running Prisma migrations from main app..."
/opt/sca/app/node_modules/.bin/prisma migrate deploy --schema=/opt/sca/api/prisma/schema.prisma || echo "Warning: Prisma migrations failed or not needed"

echo "db_sandbox container is ready!"

# Execute the command passed to docker run
exec "$@"


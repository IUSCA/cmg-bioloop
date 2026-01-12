FROM node:20

# Install PostgreSQL
RUN apt-get update && apt-get install -y \
    postgresql-15 \
    postgresql-client-15 \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /opt/sca/app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy application files
COPY . .

# Note: Prisma Client will be generated at runtime in entrypoint.sh
# after the main app's schema is mounted

# Create directory for logs
RUN mkdir -p /opt/sca/data/logs

# Expose PostgreSQL port (internal to container)
EXPOSE 5432

# Set up entrypoint
COPY bin/entrypoint.sh /opt/bin/entrypoint.sh
RUN chmod +x /opt/bin/entrypoint.sh

ENTRYPOINT ["/opt/bin/entrypoint.sh"]
CMD ["tail", "-f", "/dev/null"]


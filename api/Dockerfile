FROM node:19

ARG APP_GID
ARG APP_UID 

# create appuser and appgroup with the same ids as in the production VM.
RUN groupadd -o --gid ${APP_GID} appgroup
# echo "created appgroup with id ${APP_GID}"
RUN useradd -ms /bin/bash --uid ${APP_UID} --gid ${APP_GID} appuser
# echo "created appuser with id ${APP_UID}"

WORKDIR /opt/sca/app
RUN chown -R appuser:appgroup /opt/sca/app
# echo "changed ownership of /opt/sca/app to appuser:appgroup"

# execute below steps as
USER appuser:appgroup
# echo "executing below steps as appuser:appgroup"

# create node_modules directory before the external / docker volume mounts
# otherwise docker daemon will attempt to create this directory and it will be owned by root:root.
RUN mkdir -p /opt/sca/app/node_modules
# echo "created node_modules directory"

# setup for installing npm global packages as non-root user
RUN npm config set prefix '/home/appuser/.local/'
RUN mkdir -p /home/appuser/.local/bin
ENV PATH="/home/appuser/.local/bin/:${PATH}"
ENV NPM_CONFIG_CACHE=/home/appuser/.npm
RUN rm -rf /home/appuser/.npm \
  && mkdir -p /home/appuser/.npm/_cacache/content-v2/sha512 \
  /home/appuser/.npm/_cacache/index-v5 \
  /home/appuser/.npm/_logs

# Copy package files for dependency installation
COPY --chown=appuser:appgroup package*.json ./

# Install dependencies during build phase
RUN npm ci --cache=/home/appuser/.npm --no-audit --prefer-online

# Copy the rest of the application code
COPY --chown=appuser:appgroup . .

# Generate Prisma client during build phase
RUN npx prisma generate client

# Run the application
ENTRYPOINT exec /opt/sca/app/node_modules/.bin/nodemon --signal SIGTERM src/index.js

# RUN npm install -g pnpm
# ENTRYPOINT npm ci && npx prisma generate client && exec pm2-runtime ecosystem.config.js
# ENTRYPOINT pnpm fetch && npx prisma db push && exec /opt/sca/app/node_modules/.bin/nodemon --signal SIGTERM src/index.js

# Entrypoint will be run in the shell mode - https://docs.docker.com/engine/reference/builder/#shell-form-entrypoint-example
# any CMD or docker run command line arguments or command in docker-compose are ignored
# use --entrypoint or entrypoint in docker-compose to override this.


# pnpm fetch
#   In order to make builds predictable, it is necessary to install packages using `npm ci` which uses frozen package-lock.json.
#   This file is generated automatically during `npm install` and I don't want to run npm install in the dev environment.
#   Having multiple repositories leading to increased disk usage. To address this, I employ pnpm to minimize disk usage, but unfortunately, there is no way to convert pnpm-lock.yaml to package-lock.json.

# install packages directly from package.json using npm - build is not deterministic
# ENTRYPOINT npm install && npx prisma db push && exec /opt/sca/app/node_modules/.bin/nodemon --signal SIGTERM index.js

# install packages directly from package-lock.json using npm. Make sure that the lock is up to date.
# ENTRYPOINT npm ci && npx prisma db push && exec /opt/sca/app/node_modules/.bin/nodemon --signal SIGTERM index.js


# npx prisma db push
# Pushes the schema to the database and generates the prisma client.


# exec /opt/sca/app/node_modules/.bin/nodemon --signal SIGTERM index.js
# exec - replaces the current process with the spawned process -
#      - This means /opt/sca/app/node_modules/.bin/nodemon will get the PID 1, and
#      - will be able to receive UNIX signals (SIGTERM) and perform gracious shutdown of the server.
#      - nodemon internally sends SIGTERM to the express server (index.js) which then waits for the existing connections to close before stopping the server.
#
# Both `npm run` and `npm exec` / `npx` do not pass UNIX signals. Therefore, nodemon should be called directly using the full path /opt/sca/app/node_modules/.bin/nodemon
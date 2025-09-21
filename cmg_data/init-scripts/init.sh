#!/bin/bash
set -e

echo "Creating user appuser in cmg database"
mongosh --eval '
  db.getSiblingDB("cmg").createUser({
    user: "appuser",
    pwd: "example",
    roles: [
      {
        role: "readWrite",
        db: "cmg"
      }
    ]
  });
'

echo "Restoring MongoDB data from dump"
echo "Username: appuser"
echo "Password: example"
echo "Authentication Database: cmg"
echo "Dump Directory: /docker-entrypoint-initdb.d/dump"

echo "Restoring MongoDB data from dump"

mongorestore \
 --username appuser \
 --password example \
 --authenticationDatabase cmg \
 --db cmg \
 --dir /docker-entrypoint-initdb.d/dump \
 --drop

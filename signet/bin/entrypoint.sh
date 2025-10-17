#!/bin/bash

echo "Running entrypoint script for signet container"

if [ -f "keys/auth.key" ] && [ -f "keys/auth.pub" ]; then 
  echo "Keys already exist. Skipping key generation."
else
  echo "Generating signet keys in keys directory"
  cd keys/
  ./genkeys.sh
  cd ../
  echo "Generated signet keys in keys directory"
fi

echo "Completed entrypoint script for signet container"

$*

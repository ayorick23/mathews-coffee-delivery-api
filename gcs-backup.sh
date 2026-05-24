#!/bin/sh

# Autenticación en GCP usando la llave montada en el contenedor
gcloud auth activate-service-account --key-file=/keys/gcp-key.json --project=mathews-coffe

# Variables de tiempo y archivo
FECHA=$(date +%Y-%m-%d-%H-%M-%S)
ARCH="/tmp/backup-cassandra-${FECHA}.tar.gz"

echo "Starting backup compression to ${ARCH}..."
tar -czf "${ARCH}" -C /var/lib/cassandra .

echo "Uploading to GCS bucket: backups_cassandra..."
gsutil cp "${ARCH}" gs://backups_cassandra/

echo "Cleaning up local file..."
rm -f "${ARCH}"

echo "Backup complete!"
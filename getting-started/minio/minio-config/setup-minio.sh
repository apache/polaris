#!/bin/sh
set -e

echo "Waiting for Minio service to start..."
attempt_counter=0
max_attempts=20
until curl -s -f http://minio:9000/minio/health/live > /dev/null; do
  if [ ${attempt_counter} -eq ${max_attempts} ]; then
    echo "Max attempts reached. Failed to connect to Minio."
    exit 1
  fi
  echo "Attempting to connect to Minio (${attempt_counter}/${max_attempts})..."
  attempt_counter=$((attempt_counter+1))
  sleep 3
done
echo "Minio service is live."

mc alias set myminio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}
mc mb myminio/polaris-bucket --ignore-existing

# Create Minio policies from JSON files
mc admin policy create myminio polaris-s3-rw-policy /config/polaris-s3-rw-policy.json
mc admin policy create myminio spark-minio-rw-policy /config/spark-minio-rw-policy.json
mc admin policy create myminio trino-minio-ro-policy /config/trino-minio-ro-policy.json

# Create Minio user for Polaris Service (R/W)
mc admin user add myminio ${POLARIS_S3_USER} ${POLARIS_S3_PASSWORD}
mc admin policy attach myminio polaris-s3-rw-policy --user ${POLARIS_S3_USER}

# Create Minio user for Spark Engine data access (R/W)
mc admin user add myminio ${SPARK_MINIO_S3_USER} ${SPARK_MINIO_S3_PASSWORD}
mc admin policy attach myminio spark-minio-rw-policy --user ${SPARK_MINIO_S3_USER}

# Create Minio user for Trino Engine data access (R/O)
mc admin user add myminio ${TRINO_MINIO_S3_USER} ${TRINO_MINIO_S3_PASSWORD}
mc admin policy attach myminio trino-minio-ro-policy --user ${TRINO_MINIO_S3_USER}

echo "Minio setup complete: users (polaris_s3_user, spark_minio_s3_user, trino_minio_s3_user) and policies configured."

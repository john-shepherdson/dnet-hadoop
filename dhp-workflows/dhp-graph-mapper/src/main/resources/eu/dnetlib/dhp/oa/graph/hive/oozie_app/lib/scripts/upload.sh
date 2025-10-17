#!/bin/bash

# Check for required arguments
if [ "$#" -ne 5 ]; then
  echo "Usage: $0 <S3_ACCESS_KEY> <S3_SECRET_KEY> <S3_ENDPOINT> <SRC_PATH> <DST_PATH>"
  exit 1
fi

# Assign and export credentials
export S3_ACCESS_KEY="$1"
export S3_SECRET_KEY="$2"

# Assign other parameters
S3_ENDPOINT="$3"
SRC_PATH="$4"
DST_PATH="$5"

for entity in dataset datasource organization otherresearchproduct person project publication relation software
do
  HADOOP_USER_NAME=hdfs \
  hadoop distcp \
    -Dfs.s3a.endpoint=$S3_ENDPOINT \
    -Dfs.s3a.access.key=$S3_ACCESS_KEY \
    -Dfs.s3a.secret.key=$S3_SECRET_KEY \
    -Dfs.s3a.path.style.access=true \
    -Dfs.s3a.multipart.size=512M -Dfs.s3a.multipart.threshold=512M -Dcom.amazonaws.sdk.s3.defaultStreamBufferSize=1048576 \
    -Dfs.s3a.change.detection.source=versionId -Dfs.s3a.multipart.purge=true -Dfs.s3a.multipart.purge.age=86400  \
    -update -delete \
    ${SRC_PATH}/${entity}/ \
    ${DST_PATH}/${entity}/
done
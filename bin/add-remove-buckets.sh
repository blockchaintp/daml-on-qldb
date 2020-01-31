#!/bin/bash
# Continually add and remove S3 buckets. Use while testing to stress S3 environment.
project=${PWD##*/}
user=${USER}
bucketname=btp-${project}-${user}
bucketcount=5
while true; do
    for ((instance=1; instance<=${bucketcount}; instance++)); do
        aws s3 mb s3://${bucketname}-${instance}
    done
    sleep 1
    for ((instance=1; instance<=${bucketcount}; instance++)); do
        aws s3 rb --force s3://${bucketname}-${instance}
    done
    sleep 1
done

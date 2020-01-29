#!/bin/bash
set -e
mkdir -p $HOME/.aws
echo "[default]" > $HOME/.aws/config
echo "region = ${AWS_REGION}" >> $HOME/.aws/config
echo "[default]" > $HOME/.aws/credentials
echo "aws_access_key_id = ${AWS_ACCESS_KEY_ID}" >> $HOME/.aws/credentials
echo "aws_secret_access_key = ${AWS_SECRET_ACCESS_KEY}" >> $HOME/.aws/credentials

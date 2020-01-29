#!/bin/bash
mkdir -p $HOME/.aws && \
echo -n $'[default]\n' > $HOME/.aws/config && \
echo -n $'region = ${AWS_REGION}\n' >> $HOME/.aws/config && \
echo -n $'[default]\n' > $HOME/.aws/credentials && \
echo -n $'aws_access_key_id = ${AWS_ACCESS_KEY_ID}\n' >> $HOME/.aws/credentials && \
echo -n $'aws_secret_access_key = ${AWS_SECRET_ACCESS_KEY}\n' >> $HOME/.aws/credentials \

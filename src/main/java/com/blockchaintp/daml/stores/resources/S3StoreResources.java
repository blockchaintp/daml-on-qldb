/*
 * Copyright 2021 Blockchain Technology Partners
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.blockchaintp.daml.stores.resources;

import java.util.concurrent.CompletableFuture;

import com.blockchaintp.utility.Aws;

import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

/**
 * Handles the creation and destruction of S3 resources.
 */
public class S3StoreResources implements RequiresAWSResources {
  private static final int BUCKET_WAIT_TIME = 1000;
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(S3StoreResources.class);
  private final String bucketName;
  private final S3AsyncClient client;

  /**
   * Creates an S3StoreResources with the specified client.
   *
   * @param awsClient
   *          the AWS S3 client
   * @param storeName
   *          the name of the S3Store
   * @param tableName
   *          the table name within the store.
   */
  public S3StoreResources(final S3AsyncClient awsClient, final String storeName, final String tableName) {
    this.client = awsClient;

    this.bucketName = Aws.complyWithS3BucketNaming("vs-" + storeName + "-table-" + tableName);
  }

  private boolean bucketExists() {
    LOG.debug("Check bucket {} exists", () -> bucketName);
    var ourBucket = client.listBuckets()
        .thenApply(r -> r.buckets().stream().filter(b -> b.name().equals(bucketName)).findAny()).join();

    return ourBucket.isPresent();
  }

  @Override
  public final void ensureResources() {
    if (bucketExists()) {
      LOG.debug("Bucket {} exists, skip create", () -> bucketName);
      return;
    }

    LOG.info("Creating bucket {}", () -> bucketName);

    client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build()).join();

    while (!bucketExists()) {
      try {
        LOG.trace("Bucket {} still does not exist sleeping for {}ms", bucketName, BUCKET_WAIT_TIME);
        Thread.sleep(BUCKET_WAIT_TIME);
      } catch (InterruptedException e) {
        LOG.info("Interrupted while waiting for bucket {}", this.bucketName);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public final void destroyResources() {
    if (!bucketExists()) {
      LOG.debug("Bucket {} does not exist, skip delete", () -> bucketName);
      return;
    }
    LOG.info("Deleting bucket {}", () -> bucketName);

    // For test purposes, no need to mess about with pagination
    var keys = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build()).join();

    CompletableFuture.allOf(keys.contents().stream()
        .map(k -> client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(k.key()).build()))
        .toArray(CompletableFuture[]::new)).join();

    client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build()).join();
  }
}

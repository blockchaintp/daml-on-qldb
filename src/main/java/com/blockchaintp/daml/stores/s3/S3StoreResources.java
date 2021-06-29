package com.blockchaintp.daml.stores.s3;

import com.blockchaintp.daml.stores.RequiresAWSResources;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.util.concurrent.CompletableFuture;

public class S3StoreResources implements RequiresAWSResources {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(S3StoreResources.class);
  private final String bucketName;
  private S3AsyncClient client;

  public S3StoreResources(S3AsyncClient client, String ledgerName, String tableName) {
    this.client = client;
    this.bucketName = "valuestore-ledger-" + ledgerName + "-table-" + tableName;
  }

  private boolean bucketExists() {
    LOG.debug("Check bucket {} exists", ()-> bucketName);
    var ourBucket = client.listBuckets()
      .thenApply(r ->
        r.buckets()
          .stream()
          .filter(b -> b.name().equals(bucketName))
          .findAny()
      ).join();

    return ourBucket.isPresent();
  }

  @Override
  public void ensureResources() {
    if (bucketExists()) {
      LOG.debug("Bucket {} exists, skip create", () -> bucketName);
      return;
    }

    LOG.info("Creating bucket {}", () -> bucketName);

    var bucketCreation = client.createBucket(CreateBucketRequest
      .builder()
      .bucket(bucketName)
      .build())
      .join();
  }

  @Override
  public void destroyResources() {
    if (!bucketExists()) {
      LOG.debug("Bucket {} does not exist, skip delete", () -> bucketName);
      return;
    }
    LOG.info("Deleting bucket {}", () -> bucketName);

    //For test purposes, no need to mess about with pagination
    var keys = client.listObjectsV2(ListObjectsV2Request
      .builder()
      .bucket(bucketName)
      .build())
      .join();

    CompletableFuture.allOf(
      (CompletableFuture<DeleteBucketRequest>[])
      keys
        .contents()
        .stream()
        .map(k ->
      client.deleteBucket(DeleteBucketRequest
        .builder()
        .bucket(k.key())
        .build()))
      .toArray(CompletableFuture[]::new)
    ).join();

    var bucketDeletion = client.deleteBucket(
      DeleteBucketRequest
        .builder()
        .bucket(bucketName)
        .build()
    ).join();
  }
}

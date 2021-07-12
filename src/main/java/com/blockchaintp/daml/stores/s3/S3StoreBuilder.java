package com.blockchaintp.daml.stores.s3;

import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.function.UnaryOperator;

public class S3StoreBuilder {

  private S3AsyncClientBuilder client;
  private String ledgerName;
  private UnaryOperator<PutObjectRequest.Builder> putModifications = x -> x;
  private UnaryOperator<GetObjectRequest.Builder> getModifications = x -> x;
  private String tableName;

  public S3StoreBuilder(S3AsyncClientBuilder client) {
    this.client = client;
  }

  public S3StoreBuilder forLedger(String ledgerName) {
    this.ledgerName = ledgerName;
    return this;
  }

  public S3StoreBuilder forTable(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public S3StoreBuilder onPut(UnaryOperator<PutObjectRequest.Builder> putModifications) {
    this.putModifications = putModifications;

    return this;
  }

  public S3StoreBuilder onGet(UnaryOperator<GetObjectRequest.Builder> getModifications) {
    this.getModifications = getModifications;

    return this;
  }

  public S3Store build() throws S3StoreBuilderException {
    if (this.ledgerName == null) {
      throw new S3StoreBuilderException("Ledger name mjust be specified");
    }

    if (this.tableName == null) {
      throw new S3StoreBuilderException("Table name mjust be specified");
    }

    return new S3Store(
      this.ledgerName,
      this.tableName,
      this.client,
      this.getModifications,
      this.putModifications
    );
  }
}

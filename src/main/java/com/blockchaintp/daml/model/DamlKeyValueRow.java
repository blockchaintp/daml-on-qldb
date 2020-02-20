package com.blockchaintp.daml.model;

import java.io.IOException;

import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;

public interface DamlKeyValueRow {

  String getId();

  void setId(String id);

  String getS3Key();

  void setS3Key(String key);

  byte[] s3Data();

  void refreshFromBulkStore() throws IOException;

  void updateBulkStore() throws IOException;

  Result insert(TransactionExecutor txn) throws IOException;

  Result update(TransactionExecutor txn) throws IOException;

  DamlKeyValueRow fetch(TransactionExecutor txn) throws IOException;

  boolean exists(TransactionExecutor txn) throws IOException;

  boolean upsert(TransactionExecutor txn) throws IOException;

  boolean delete(TransactionExecutor txn) throws IOException;

  String tableName();

}

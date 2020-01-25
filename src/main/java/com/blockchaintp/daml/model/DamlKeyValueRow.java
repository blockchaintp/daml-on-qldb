package com.blockchaintp.daml.model;

import java.io.IOException;

import software.amazon.qldb.Result;
import software.amazon.qldb.Transaction;

public interface DamlKeyValueRow {

  String getId();

  void setId(String id);

  String getS3Key();

  void setS3Key(String key);

  byte[] s3Data();

  void refreshFromBulkStore() throws IOException;

  void updateBulkStore() throws IOException;

  Result insert(Transaction txn) throws IOException;

  Result update(Transaction txn) throws IOException;

  DamlKeyValueRow fetch(Transaction txn) throws IOException;

  boolean exists(Transaction txn) throws IOException;

  boolean upsert(Transaction txn) throws IOException;

  boolean delete(Transaction txn) throws IOException;

  String tableName();
  
}

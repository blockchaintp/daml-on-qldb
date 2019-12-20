package com.blockchaintp.daml.model;

import java.io.IOException;

import com.blockchaintp.daml.DamlLedger;

import software.amazon.qldb.Result;
import software.amazon.qldb.Transaction;

public interface DamlKeyValueRow {

  String getId();

  void setId(String id);

  String getS3Key();

  void setS3Key(String key);

  byte[] s3Data();

  void refreshFromBulkStore(DamlLedger ledger) throws IOException;

  void updateBulkStore(DamlLedger ledger) throws IOException;

  Result insert(Transaction txn, DamlLedger ledger) throws IOException;

  Result update(Transaction txn, DamlLedger ledger) throws IOException;

  DamlKeyValueRow fetch(Transaction txn, DamlLedger ledger) throws IOException;

  boolean exists(Transaction txn) throws IOException;

  boolean upsert(Transaction txn, DamlLedger ledger) throws IOException;

  boolean delete(Transaction txn, DamlLedger ledger) throws IOException;

  String tableName();

}

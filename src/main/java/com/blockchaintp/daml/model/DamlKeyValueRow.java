package com.blockchaintp.daml.model;

import java.io.IOException;

import com.blockchaintp.daml.DamlLedger;

import software.amazon.qldb.Result;
import software.amazon.qldb.Transaction;

public interface DamlKeyValueRow {
  public String damlkey();

  public String s3Key();

  public String data();

  public Result insert(Transaction txn, DamlLedger ledger) throws IOException;

  public Result update(Transaction txn, DamlLedger ledger) throws IOException;

  public DamlKeyValueRow fetch(Transaction txn, DamlLedger ledger) throws IOException;

  public boolean exists(Transaction txn) throws IOException;

  public boolean upsert(Transaction txn, DamlLedger ledger) throws IOException;

  public boolean delete(Transaction txn, DamlLedger ledger) throws IOException;

  public String tableName();

}

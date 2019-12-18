package com.blockchaintp.daml.model;

import java.io.IOException;

import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;

public interface DamlKeyValueRow {
  public String damlkey();

  public String data();

  public Result insert(TransactionExecutor txn) throws IOException;

  public Result update(TransactionExecutor txn) throws IOException;

  public DamlKeyValueRow fetch(TransactionExecutor txn) throws IOException;

  public boolean exists(TransactionExecutor txn) throws IOException;

  public boolean upsert(TransactionExecutor txn) throws IOException;

  public boolean delete(TransactionExecutor txn) throws IOException;

  public String tableName();

}

package com.blockchaintp.daml.stores.qldb;

import software.amazon.qldb.QldbDriver;

public class QldbStoreBuilder {

  private final QldbDriver driver;
  private String table;

  private QldbStoreBuilder(QldbDriver driver) {
    this.driver = driver;
  }

  public static QldbStoreBuilder forDriver(QldbDriver driver) {
    return new QldbStoreBuilder(driver);
  }

  public QldbStoreBuilder tableName(String table) {
    this.table = table;
    return this;
  }

  public QldbStore build() throws QldbStoreBuilderException {
    if (table == null) {
      throw new QldbStoreBuilderException("No table name specfified in builder");
    }
    return new QldbStore(
      driver,
      table
    );
  }
}

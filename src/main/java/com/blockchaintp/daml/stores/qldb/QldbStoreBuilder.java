package com.blockchaintp.daml.stores.qldb;

import software.amazon.qldb.QldbDriver;

public final class QldbStoreBuilder {

  private final QldbDriver driver;
  private String table;

  private QldbStoreBuilder(final QldbDriver qldbDriver) {
    this.driver = qldbDriver;
  }

  public static QldbStoreBuilder forDriver(final QldbDriver driver) {
    return new QldbStoreBuilder(driver);
  }

  public QldbStoreBuilder tableName(final String tableName) {
    this.table = tableName;
    return this;
  }

  public QldbStore build() throws QldbStoreBuilderException {
    if (table == null) {
      throw new QldbStoreBuilderException("No table name specified in builder");
    }
    return new QldbStore(
      driver,
      table
    );
  }
}

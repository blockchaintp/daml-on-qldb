package com.blockchaintp.daml.stores.qldb;

import software.amazon.qldb.QldbDriver;

/**
 * A builder of QLDBStore instances.
 */
public final class QldbStoreBuilder {

  private final QldbDriver driver;
  private String table;

  private QldbStoreBuilder(final QldbDriver qldbDriver) {
    this.driver = qldbDriver;
  }

  /**
   * Use the given QLDB driver.
   * @param driver the driver
   * @return the builder
   */
  public static QldbStoreBuilder forDriver(final QldbDriver driver) {
    return new QldbStoreBuilder(driver);
  }

  /**
   * Use the given table name.
   * @param tableName the table name
   * @return the builder
   */
  public QldbStoreBuilder tableName(final String tableName) {
    this.table = tableName;
    return this;
  }

  /**
   * Construct a QLDBStore instance.
   * @return the instance
   */
  public QldbStore build() {
    if (table == null) {
      throw new QldbStoreBuilderException("No table name specified in builder");
    }
    return new QldbStore(
      driver,
      table
    );
  }
}

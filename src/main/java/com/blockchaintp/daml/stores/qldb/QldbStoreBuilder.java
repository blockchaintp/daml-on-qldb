/*
 * Copyright 2021 Blockchain Technology Partners
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
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
   *
   * @param driver
   *          the driver
   * @return the builder
   */
  public static QldbStoreBuilder forDriver(final QldbDriver driver) {
    return new QldbStoreBuilder(driver);
  }

  /**
   * Use the given table name.
   *
   * @param tableName
   *          the table name
   * @return the builder
   */
  public QldbStoreBuilder tableName(final String tableName) {
    this.table = tableName;
    return this;
  }

  /**
   * Construct a QLDBStore instance.
   *
   * @return the instance
   */
  public QldbStore build() {
    if (table == null) {
      throw new QldbStoreBuilderException("No table name specified in builder");
    }
    return new QldbStore(driver, table);
  }
}

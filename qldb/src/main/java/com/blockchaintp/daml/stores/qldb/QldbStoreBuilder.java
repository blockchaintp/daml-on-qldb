/*
 * Copyright © 2023 Paravela Limited
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

import com.blockchaintp.daml.stores.layers.RetryingConfig;
import com.blockchaintp.daml.stores.service.Store;
import com.google.protobuf.ByteString;

import software.amazon.qldb.QldbDriver;

/**
 * A builder of QLDBStore instances.
 */
public final class QldbStoreBuilder {

  private final QldbDriver driver;
  private String table;
  private RetryingConfig retryingConfig;

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
   * Specify the number of retries to use for the stores built.
   *
   * @param maxRetries
   *          the maximum number of retries.
   * @return the builder
   */
  public QldbStoreBuilder retrying(final int maxRetries) {
    this.retryingConfig = new RetryingConfig();
    this.retryingConfig.setMaxRetries(maxRetries);

    return this;
  }

  /**
   * Construct a QLDBStore instance.
   *
   * @return the instance
   */
  public Store<ByteString, ByteString> build() {
    if (table == null) {
      throw new QldbStoreBuilderException("No table name specified in builder");
    }
    var store = new QldbStore(driver, table);

    if (retryingConfig != null) {
      return new QldbRetryStrategy<>(retryingConfig, store);
    }

    return store;
  }
}

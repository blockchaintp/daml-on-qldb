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
package com.blockshaintp.daml.stores.postgres;

import java.sql.DriverManager;
import java.sql.SQLException;

import com.blockchaintp.daml.stores.layers.RetryingConfig;
import com.blockchaintp.daml.stores.layers.RetryingStore;
import com.blockchaintp.daml.stores.service.Store;
import com.google.protobuf.ByteString;

import org.flywaydb.core.Flyway;

/**
 * A builder for postgrestore instances.
 */
public final class PostgresStoreBuilder {
  private final String url;
  private RetryingConfig retryingConfig;

  /**
   *
   * @param theUrl
   */
  public PostgresStoreBuilder(final String theUrl) {
    url = theUrl;
  }

  /**
   * Migrate database at url.
   *
   * @return A configured builder.
   */
  public PostgresStoreBuilder migrate() {
    var flyway = Flyway.configure().locations("classpath:migrations/store").dataSource(url, "", "").load();

    flyway.migrate();

    return this;
  }

  /**
   * Specify the number of retries to use for the stores built.
   *
   * @param maxRetries
   *          the maximum number of retries.
   * @return the builder
   */
  public PostgresStoreBuilder retrying(final int maxRetries) {
    this.retryingConfig = new RetryingConfig();
    this.retryingConfig.setMaxRetries(maxRetries);

    return this;
  }

  /**
   * @return A configured store.
   */
  public Store<ByteString, ByteString> build() throws SQLException {
    var connection = DriverManager.getConnection(url);
    var store = new PostgresStore(connection);

    if (retryingConfig != null) {
      return new RetryingStore<>(retryingConfig, store);
    }

    return store;
  }
}

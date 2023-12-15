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
package com.blockshaintp.daml.stores.postgres;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

import com.blockchaintp.daml.stores.service.TransactionLog;
import com.google.protobuf.ByteString;

import org.flywaydb.core.Flyway;

/**
 * Builder for postgres based transaction logs.
 */
public final class PostgresTransactionLogBuilder {
  private final String url;

  /**
   *
   * @param theUrl
   */
  public PostgresTransactionLogBuilder(final String theUrl) {
    url = theUrl;
  }

  /**
   * Migrate database at url.
   *
   * @return A configured builder.
   */
  public PostgresTransactionLogBuilder migrate() {
    var flyway = Flyway.configure().locations("classpath:migrations/txlog").dataSource(url, "", "").load();

    flyway.migrate();

    return this;
  }

  /**
   * @return A configured txlog.
   */
  public TransactionLog<UUID, ByteString, Long> build() throws SQLException {
    var connection = DriverManager.getConnection(url);
    return new PostgresTransactionLog(connection);
  }
}

/*
 * Copyright 2021-2022 Blockchain Technology Partners
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
package postgres;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Store;
import com.blockshaintp.daml.stores.postgres.PostgresTransactionLog;
import com.google.protobuf.ByteString;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;

@Disabled
class PostgresTxLogIntegrationTest {
  private static final int ITERATIONS = 40;
  private Store<ByteString, ByteString> store;
  private PostgresTransactionLog txLog;

  @BeforeEach
  final void establishStore() throws SQLException, IOException {
    var pg = EmbeddedPostgres.builder().start();
    pg.getPostgresDatabase().setLogWriter(new PrintWriter(System.out, true));
    var connection = pg.getPostgresDatabase().getConnection();
    Flyway.configure().locations("classpath:migrations/txlog").dataSource(pg.getPostgresDatabase()).load().migrate();

    txLog = new PostgresTransactionLog(connection);
  }

  @Test
  final void committed_transactions_are_read_in_commit_order()
      throws StoreWriteException, StoreReadException, InterruptedException {
    var ids = new ArrayList<UUID>();

    for (var i = 0; i != 3000; i++) {
      var id = txLog.begin(Optional.of(UUID.randomUUID()));
      ids.add(id._1);
    }
    var futures = new ArrayList<Future<?>>();

    var executor = Executors.newFixedThreadPool(30);
    ids.stream().forEach(id -> {
      futures.add(executor.submit(() -> {
        try {
          sleepFor((long) Math.random() % 30);
          txLog.sendEvent(id, ByteString.copyFromUtf8("testdata"));
          txLog.commit(id);
        } catch (StoreWriteException theE) {
          theE.printStackTrace();
        }
      }));
    });

    this.sleepFor(10000);

    var aborted = txLog.begin(Optional.empty());
    txLog.sendEvent(aborted._1, ByteString.copyFromUtf8("aborted"));
    txLog.abort(aborted._1);

    var stream = txLog.from(-1L, Optional.empty());

    var page = stream.limit(3000).collect(Collectors.toList());

    Assertions.assertIterableEquals(ids, page.stream().map(x -> x._2).collect(Collectors.toList()));
  }

  private void sleepFor(long millis) {
    long stopAfter = System.currentTimeMillis() + (long) (Math.random() % 30);
    Awaitility.await().until(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        return System.currentTimeMillis() > stopAfter;
      }
    });
  }
}

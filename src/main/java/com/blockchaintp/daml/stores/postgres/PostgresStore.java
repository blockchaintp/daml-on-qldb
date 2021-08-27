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
package com.blockchaintp.daml.stores.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.layers.WrapFunction0;
import com.blockchaintp.daml.stores.layers.WrapRunnable;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;

import io.vavr.CheckedFunction0;
import io.vavr.CheckedRunnable;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;

/**
 * A PostgresStore is a store backed by a postgres interface.
 */
public class PostgresStore implements Store<ByteString, ByteString> {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(PostgresStore.class);
  private final Connection connection;

  /**
   * @param url
   * @return A builder based on this connection string.
   */
  public static PostgresStoreBuilder fromUrl(final String url) {
    return new PostgresStoreBuilder(url);
  }

  /**
   *
   * @param theConnection
   */
  public PostgresStore(final Connection theConnection) {
    connection = theConnection;
  }

  private <T> T guardRead(final CheckedFunction0<T> op) throws StoreReadException {
    return WrapFunction0.of(() -> op.unchecked().get(), e -> new StoreReadException(e)).apply();
  }

  private void guardWrite(final CheckedRunnable op) throws StoreWriteException {
    WrapRunnable.of(() -> op.unchecked().run(), e -> new StoreWriteException(e)).run();
  }

  private ResultSet getByKey(final List<Key<ByteString>> keys) throws SQLException {
    var stmt = connection.prepareStatement("select id,data from kv where id = any((?))",
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    var blobs = new ArrayList<byte[]>();

    for (Key<ByteString> key : keys) {
      blobs.add(key.toNative().toByteArray());
    }
    var in = connection.createArrayOf("bytea", blobs.toArray(new byte[blobs.size()][]));
    stmt.setArray(1, in);

    LOG.debug("Execute {}", stmt);

    return stmt.executeQuery();
  }

  @Override
  public final Optional<Value<ByteString>> get(final Key<ByteString> key) throws StoreReadException {
    return guardRead(() -> {
      var rx = getByKey(Arrays.asList(key));

      if (!rx.first()) {
        return Optional.empty();
      }

      var ret = Optional.of(Value.of(ByteString.readFrom(rx.getBinaryStream("data"))));

      rx.close();

      return ret;
    });
  }

  @Override
  public final Map<Key<ByteString>, Value<ByteString>> get(final List<Key<ByteString>> listOfKeys)
      throws StoreReadException {
    return guardRead(() -> {
      var rx = getByKey(listOfKeys);
      var map = new HashMap<Key<ByteString>, Value<ByteString>>();

      if (rx.first()) {
        do {
          map.put(Key.of(ByteString.readFrom(rx.getBinaryStream("id"))),
              Value.of(ByteString.readFrom(rx.getBinaryStream("data"))));
        } while (rx.next());
      }
      rx.close();

      return map;
    });
  }

  private int setByKey(final List<Map.Entry<Key<ByteString>, Value<ByteString>>> listOfPairs) throws SQLException {
    /// De-duplicate input list by key, taking last value
    var toPut = listOfPairs.stream().collect(Collectors.toMap(kv -> kv.getKey(), kv -> kv.getValue(), (l, r) -> r))
        .entrySet().stream().collect(Collectors.toList());

    var stmt = connection.prepareStatement(
        "insert into kv(id,data) select unnest((?)),unnest((?)) on conflict(id) do update set data = excluded.data");
    var keys = new ArrayList<byte[]>();
    var values = new ArrayList<byte[]>();

    for (var kv : toPut) {
      keys.add(kv.getKey().toNative().toByteArray());
      values.add(kv.getValue().toNative().toByteArray());
    }
    stmt.setArray(1, connection.createArrayOf("bytea", keys.toArray(new byte[keys.size()][])));
    stmt.setArray(2, connection.createArrayOf("bytea", values.toArray(new byte[values.size()][])));

    LOG.debug("Execute {}", stmt);

    var rows = stmt.executeUpdate();

    return rows;
  }

  @Override
  public final void put(final Key<ByteString> key, final Value<ByteString> value) throws StoreWriteException {
    guardWrite(() -> setByKey(Arrays.asList(Map.entry(key, value))));
  }

  @Override
  public final void put(final List<Map.Entry<Key<ByteString>, Value<ByteString>>> listOfPairs)
      throws StoreWriteException {
    guardWrite(() -> setByKey(listOfPairs));
  }
}

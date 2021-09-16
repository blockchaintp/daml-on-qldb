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

import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.resources.QldbResources;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Opaque;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.Value;
import com.blockchaintp.utility.Aws;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.qldb.QldbClient;
import software.amazon.awssdk.services.qldbsession.QldbSessionClient;
import software.amazon.qldb.QldbDriver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

class QldbStoreIntegrationTest {
  private static final int ITERATIONS = 40;
  private Store<ByteString, ByteString> store;
  private IonSystem ionSystem;
  private QldbResources resources;

  @BeforeEach
  final void establishStore() throws StoreWriteException {
    String ledger = UUID.randomUUID().toString().replace("-", "");

    final var sessionBuilder = QldbSessionClient.builder().region(Region.EU_WEST_2)
        .credentialsProvider(DefaultCredentialsProvider.builder().build());

    this.ionSystem = IonSystemBuilder.standard().build();

    final var driver = QldbDriver.builder().ledger(Aws.complyWithQldbLedgerNaming(ledger))
        .sessionClientBuilder(sessionBuilder).ionSystem(ionSystem).build();

    this.resources = new QldbResources(
        QldbClient.builder().credentialsProvider(DefaultCredentialsProvider.create()).region(Region.EU_WEST_2).build(),
        ledger, false);

    final var storeBuilder = QldbStore.forDriver(driver).tableName("qldbstoreintegrationtest");

    this.store = storeBuilder.build();

    resources.destroyResources();
    resources.ensureResources();
  }

  @AfterEach
  final void dropStore() {
    resources.destroyResources();
  }

  @Test
  void get_non_existent_items_returns_none() throws StoreReadException {
    Assertions.assertEquals(Optional.empty(), store.get(Key.of(ByteString.copyFromUtf8("nothere"))));

    var params = new ArrayList<Key<ByteString>>();
    params.add(Key.of(ByteString.copyFromUtf8("nothere")));

    Assertions.assertEquals(Map.of(), store.get(params));
  }

  @Test
  void single_item_put_and_get_are_symmetric() throws StoreWriteException, StoreReadException {
    final var id = UUID.randomUUID();
    final var k = Key.of(ByteString.copyFromUtf8(id.toString()));
    final var v = Value.of(ByteString.copyFromUtf8("test"));

    // Insert
    store.put(k, v);
    Assertions.assertArrayEquals(ByteString.copyFromUtf8("test").toByteArray(),
        store.get(k).get().toNative().toByteArray());

    final var v2 = Value.of(ByteString.copyFromUtf8("test2"));

    // Update
    store.put(k, v2);
    Assertions.assertEquals(ByteString.copyFromUtf8("test2"), store.get(k).get().toNative());
  }

  @Test
  void multiple_item_put_and_get_are_symmetric() throws StoreWriteException, StoreReadException {
    final var map = new HashMap<Key<ByteString>, Value<ByteString>>();
    for (int i = 0; i < ITERATIONS; i++) {
      final var id = UUID.randomUUID();
      final var k = Key.of(ByteString.copyFromUtf8(String.format("\"%s\"", id)));
      final var v = Value.of(ByteString.copyFromUtf8("test"));

      map.put(k, v);
    }

    var sortedkeys = map.keySet().stream()
        .sorted(Comparator.comparing(Opaque::toNative, (l, r) -> Arrays.compare(l.toByteArray(), r.toByteArray())))
        .collect(Collectors.toList());

    // Put our initial list of values, will issue insert
    store.put(new ArrayList<>(map.entrySet()));
    var rx = store.get(sortedkeys);

    compareUpserted(map, sortedkeys, rx);

    // Put it again, will issue update
    store.put(new ArrayList<>(map.entrySet()));
    var rx2 = store.get(sortedkeys);

    compareUpserted(map, sortedkeys, rx2);
  }

  final void compareUpserted(final HashMap<Key<ByteString>, Value<ByteString>> map,
      final List<Key<ByteString>> sortedkeys, final Map<Key<ByteString>, Value<ByteString>> rx) {
    Assertions.assertIterableEquals(
        sortedkeys.stream().map(map::get).map(Opaque::toNative).collect(Collectors.toList()),
        sortedkeys.stream().map(rx::get).map(Opaque::toNative).collect(Collectors.toList()));
  }

}

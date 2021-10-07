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
package store;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.resources.S3StoreResources;
import com.blockchaintp.daml.stores.s3.S3Store;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Opaque;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

class S3StoreIntegrationTest {

  private static final int NETTY_MAX_CONCURRENCY = 100;
  private static final int ID_LENGTH = 12;
  private static final int ITERATIONS = 40;
  private Store<ByteString, ByteString> store;
  private S3StoreResources resources;

  @BeforeEach
  final void create_test_bucket() {
    var ledgerId = UUID.randomUUID().toString().substring(0, ID_LENGTH);
    var tableId = UUID.randomUUID().toString().substring(0, ID_LENGTH);

    SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder().maxConcurrency(NETTY_MAX_CONCURRENCY).build();

    var clientBuilder = S3AsyncClient.builder().httpClient(httpClient).region(Region.EU_WEST_2)
        .credentialsProvider(DefaultCredentialsProvider.builder().build());

    this.resources = new S3StoreResources(clientBuilder.build(), ledgerId, tableId);

    resources.ensureResources();

    this.store = S3Store.forClient(clientBuilder).forStore(ledgerId).forTable(tableId).build();
  }

  @AfterEach
  final void destroy_test_bucket() {
    resources.destroyResources();
  }

  @Test
  void get_non_existent_items_returns_none() throws StoreReadException {
    Assertions.assertEquals(Optional.empty(), store.get(Key.of(ByteString.copyFromUtf8("nothere"))));

    var keys = new ArrayList<Key<ByteString>>();
    keys.add(Key.of(ByteString.copyFromUtf8("nothere")));
    Assertions.assertEquals(Map.of(), store.get(keys));
  }

  @Test
  void single_item_put_and_get_are_symmetric() throws StoreWriteException, StoreReadException {
    byte[] array = new byte[4000];
    new Random().nextBytes(array);

    final var k = Key.of(ByteString.copyFrom(array));
    final var v = Value.of(ByteString.copyFromUtf8("data"));

    // Insert
    store.put(k, v);

    var getValue = store.get(k).get();
    Assertions.assertArrayEquals(getValue.toNative().toByteArray(), v.toNative().toByteArray());

    final var v2 = Value.of(ByteString.copyFromUtf8("data"));

    // Update
    store.put(k, v2);
    Optional<Value<ByteString>> justPut = store.get(k);
    Assertions.assertArrayEquals(v2.toNative().toByteArray(), justPut.get().toNative().toByteArray());
  }

  @Test
  void multiple_item_put_and_get_are_symmetric() throws StoreWriteException, StoreReadException {
    var map = new HashMap<Key<ByteString>, Value<ByteString>>();

    for (int i = 0; i != ITERATIONS; i++) {
      final var k = Key.of(ByteString.copyFromUtf8(String.format("id%d", i)));
      final var v = Value.of(ByteString.copyFromUtf8("data"));

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
    var left = sortedkeys.stream().map(map::get).map(Opaque::toNative).collect(Collectors.toList());

    var right = sortedkeys.stream().map(rx::get).map(Opaque::toNative).collect(Collectors.toList());

    for (int i = 0; i != left.size(); i++) {
      Assertions.assertArrayEquals(left.get(i).toByteArray(), right.get(i).toByteArray());
    }
  }

}

package com.blockchaintp.daml.stores.qldb;

import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.resources.QldbResources;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Value;
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

import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

class QldbStoreIntegrationTest {
  private static final int ITERATIONS = 40;
  private QldbStore store;
  private IonSystem ionSystem;
  private QldbResources resources;

  @BeforeEach
  final void establishStore() throws StoreWriteException {
    String ledger = UUID.randomUUID().toString().replace("-", "");

    final var sessionBuilder = QldbSessionClient.builder().region(Region.EU_WEST_2)
      .credentialsProvider(DefaultCredentialsProvider.builder().build());

    this.ionSystem = IonSystemBuilder.standard().build();

    final var driver = QldbDriver.builder().ledger(ledger).sessionClientBuilder(sessionBuilder).ionSystem(ionSystem)
      .build();

    this.resources = new QldbResources(
      QldbClient.builder().credentialsProvider(DefaultCredentialsProvider.create()).region(Region.EU_WEST_2).build(),
      driver, ledger, "qldbstoreintegrationtest");

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
    Assertions.assertEquals(Optional.empty(), store.get(
      Key.of(ByteString.copyFrom("nothere", Charset.defaultCharset()))));

    var params = new ArrayList<Key<ByteString>>();
    params.add(Key.of(ByteString.copyFrom("nothere", Charset.defaultCharset())));

    Assertions.assertEquals(Map.of(), store.get(params));
  }

  @Test
  void single_item_put_and_get_are_symmetric() throws StoreWriteException, StoreReadException {
    final var id = UUID.randomUUID();
    final var k = Key.of(ByteString.copyFrom(String.format("'%s'", id)
      , Charset.defaultCharset()));
    final var v = Value.of(ByteString.copyFrom("test"
      , Charset.defaultCharset()));

    // Insert
    store.put(k, v);
    Assertions.assertEquals("test", store.get(k).get().toNative().toStringUtf8());


    final var v2 = Value.of(ByteString.copyFrom("test2"
      , Charset.defaultCharset()));

    // Update
    store.put(k, v2);
    Assertions.assertEquals("test2", store.get(k).get().toNative().toStringUtf8());
  }

  @Test
  void multiple_item_put_and_get_are_symmetric() throws StoreWriteException, StoreReadException {
    final var map = new HashMap<Key<ByteString>, Value<ByteString>>();
    for (int i = 0; i < ITERATIONS; i++) {
      final var id = UUID.randomUUID();
      final var k = Key.of(ByteString.copyFrom(String.format("\"%s\"", id)
        , Charset.defaultCharset()));
      final var v = Value.of(ByteString.copyFrom("test", Charset.defaultCharset()));

      map.put(k, v);
    }

    var sortedkeys = map.keySet().stream().sorted(Comparator.comparing(o -> o.toNative().toString()))
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

  final void compareUpserted(final HashMap<Key<ByteString>, Value<ByteString>> map, final List<Key<ByteString>> sortedkeys,
                             final Map<Key<ByteString>, Value<ByteString>> rx) {
    Assertions.assertIterableEquals(
      sortedkeys
        .stream()
        .map(map::get)
        .map(Object::toString)
        .collect(Collectors.toList()),
      sortedkeys
        .stream()
        .map(rx::get)
        .map(Object::toString)
        .collect(Collectors.toList())
    );
  }

}

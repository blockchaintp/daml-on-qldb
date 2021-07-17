package com.blockchaintp.daml.stores.qldb;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.qldb.QldbClient;
import software.amazon.awssdk.services.qldbsession.QldbSessionClient;
import software.amazon.qldb.QldbDriver;

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
    Assertions.assertEquals(Optional.empty(), store.get(new Key<>(ionSystem.singleValue("nothere"))));

    var params = new ArrayList<Key<IonValue>>();
    params.add(new Key<>(ionSystem.singleValue("nothere")));

    Assertions.assertEquals(Map.of(), store.get(params));
  }

  @Test
  void single_item_put_and_get_are_symmetric() throws StoreWriteException, StoreReadException {
    final var id = UUID.randomUUID();
    final var k = new Key<>(ionSystem.singleValue(String.format("'%s'", id)));
    final var v = new Value<>((IonStruct) ionSystem.singleValue(String.format("{'id' : '%s', 'data': 232}", id)));

    // Insert
    store.put(k, v);
    Assertions.assertEquals(v.toString(), store.get(k).get().toString());

    final var v2 = new Value<>((IonStruct) ionSystem.singleValue(String.format("{'id' : '%s', 'data': 233}", id)));

    // Update
    store.put(k, v2);
    Assertions.assertEquals(v2.toString(), store.get(k).get().toString());
  }

  @Test
  void multiple_item_put_and_get_are_symmetric() throws StoreWriteException, StoreReadException {
    final var map = new HashMap<Key<IonValue>, Value<IonStruct>>();
    for (int i = 0; i < ITERATIONS; i++) {
      final var id = UUID.randomUUID();
      final Key<IonValue> k = new Key<>(ionSystem.singleValue(String.format("\"%s\"", id)));
      final var v = new Value<>((IonStruct) ionSystem.singleValue(String.format("{\"id\" : \"%s\", 'data': 237}", id)));

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

  final void compareUpserted(final HashMap<Key<IonValue>, Value<IonStruct>> map, final List<Key<IonValue>> sortedkeys,
      final Map<Key<IonValue>, Value<IonStruct>> rx) {
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

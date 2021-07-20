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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.Value;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;

import io.vavr.API;
import io.vavr.Tuple;
import io.vavr.collection.Stream;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import software.amazon.qldb.Executor;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.Result;
import software.amazon.qldb.exceptions.QldbDriverException;

/**
 * A K/V store using Amazon QLDB as a backend.
 */
public final class QldbStore implements TransactionLog<ByteString, ByteString> {

  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(QldbStore.class);
  private static final String ID_FIELD = "i";
  private static final String HASH_FIELD = "h";

  private final QldbDriver driver;
  private final String table;
  private final IonSystem ion;

  /**
   * Constructor for QldbStore.
   *
   * @param qldbDriver
   *          the driver to use
   * @param tableName
   *          the table name to use
   */
  public QldbStore(final QldbDriver qldbDriver, final String tableName) {
    this.driver = qldbDriver;
    this.table = tableName;
    this.ion = IonSystemBuilder.standard().build();
  }

  /**
   * Return a builder for the specified driver.
   *
   * @param driver
   *          the driver to use
   * @return the builder
   */
  public static QldbStoreBuilder forDriver(final QldbDriver driver) {
    return QldbStoreBuilder.forDriver(driver);
  }

  @Override
  @SuppressWarnings("java:S1905")
  public Optional<Value<ByteString>> get(final Key<ByteString> key) throws StoreReadException {
    LOG.info("get id={} in table={}", () -> key.toNative().toStringUtf8(), () -> table);

    try {
      final var r = driver.execute((Executor<Result>) ex -> ex
          .execute(String.format("select o.h from %s AS o where o.%s = ?", table, ID_FIELD), makeStorableKey(key)));

      if (!r.isEmpty()) {
        var struct = (IonStruct) r.iterator().next();
        var hash = getHashFromRecord(struct);

        return Optional.of(Value.of(ByteString.copyFrom(hash.getBytes())));
      }
      return Optional.empty();
    } catch (QldbDriverException e) {
      throw new StoreReadException(e);
    }
  }

  private IonBlob getIdFromRecord(final IonValue struct) throws StoreReadException {
    if (!(struct instanceof IonStruct)) {
      throw new StoreReadException(QldbStoreException.invalidSchema(struct));
    }
    var hash = ((IonStruct) struct).get(ID_FIELD);

    if (hash == null || !(hash instanceof IonBlob)) {
      throw new StoreReadException(QldbStoreException.invalidSchema(struct));
    }
    return (IonBlob) hash;
  }

  private IonBlob getHashFromRecord(final IonValue struct) throws StoreReadException {
    if (!(struct instanceof IonStruct)) {
      throw new StoreReadException(QldbStoreException.invalidSchema(struct));
    }
    var hash = ((IonStruct) struct).get(HASH_FIELD);

    if (hash == null || !(hash instanceof IonBlob)) {
      throw new StoreReadException(QldbStoreException.invalidSchema(struct));
    }
    return (IonBlob) hash;
  }

  @Override
  @SuppressWarnings("java:S1905")
  public Map<Key<ByteString>, Value<ByteString>> get(final List<Key<ByteString>> listOfKeys) throws StoreReadException {
    LOG.info("get ids=({}) in table={}", () -> listOfKeys.stream().map(k -> k.toNative().toStringUtf8()), () -> table);

    final var query = String.format("select o.* from %s as o where o.%s in ( %s )", table, ID_FIELD,
        listOfKeys.stream().map(k -> "?").collect(Collectors.joining(",")));

    try {
      final var r = driver.execute((Executor<Result>) ex -> ex.execute(query,
          listOfKeys.stream().map(this::makeStorableKey).toArray(IonValue[]::new)));

      /// Pull id out of the struct to use for our result map
      return Stream.ofAll(r).toJavaMap(
          k -> Tuple.of(Key.of(ByteString.copyFrom(API.unchecked(() -> getIdFromRecord(k)).get().getBytes())),
              Value.of(ByteString.copyFrom(API.unchecked(() -> getHashFromRecord(k)).get().getBytes()))));
    } catch (QldbDriverException e) {
      throw new StoreReadException(e);
    }
  }

  private IonValue makeStorableKey(final Key<ByteString> key) {
    return ion.newBlob(key.toNative().toByteArray());
  }

  private IonValue makeStorableValue(final Value<ByteString> value) {
    return ion.newBlob(value.toNative().toByteArray());
  }

  private IonStruct makeRecord(final Key<ByteString> key, final Value<ByteString> value) {
    var struct = ion.newEmptyStruct();
    struct.add(ID_FIELD, makeStorableKey(key));
    struct.add(HASH_FIELD, makeStorableValue(value));

    return struct;
  }

  /**
   * Put a single item to QLDB efficiently, conditionally and atomically using update or insert
   * depending if the item exists.
   */
  @Override
  public void put(final Key<ByteString> key, final Value<ByteString> value) throws StoreWriteException {

    LOG.info("upsert id={} in table={}", () -> key.toNative().toStringUtf8(), () -> table);

    driver.execute(tx -> {
      var exists = tx.execute(String.format("select o.%s from %s as o where o.%s = ?", ID_FIELD, table, ID_FIELD),
          makeStorableKey(key));

      if (exists.isEmpty()) {
        LOG.debug("Not present, inserting");
        var r = tx.execute(String.format("insert into %s value ?", table), makeRecord(key, value));

        LOG.debug("{}", r);
      } else {
        LOG.debug("Present, updating");
        tx.execute(String.format("update %s as o set o = ? where o.%s = ?", table, ID_FIELD), makeRecord(key, value),
            makeStorableKey(key));
      }
    });
  }

  /**
   * Put multiple items to the store as efficiently as possible. We issue a select for all the keys,
   * bulk insert those that are not present and update those that are. There are potential issues with
   * quotas @see <a href= "https://docs.aws.amazon.com/qldb/latest/developerguide/limits.html">QLDB
   * Quotas</a>
   *
   * @param listOfPairs
   *          A key / value list of ByteStrings and ByteStrings
   */
  @Override
  public void put(final List<Map.Entry<Key<ByteString>, Value<ByteString>>> listOfPairs) throws StoreWriteException {
    LOG.debug("upsert ids={} in table={}",
        () -> listOfPairs.stream().map(Map.Entry::getKey).collect(Collectors.toList()), () -> table);

    driver.execute(txn -> {
      var keys = listOfPairs.stream().map(Map.Entry::getKey).collect(Collectors.toSet());

      var exists = StreamSupport
          .stream(txn.execute(
              String.format("select o.%s from %s as o where o.%s in ( %s )", ID_FIELD, table, ID_FIELD,
                  keys.stream().map(k -> "?").collect(Collectors.joining(","))),
              keys.stream().map(this::makeStorableKey).collect(Collectors.toList())).spliterator(), false)
          .collect(Collectors.toSet());

      // results are tuples of {id,value}
      var existingKeys = exists.stream()
          .map(k -> API.unchecked(() -> Key.of(ByteString.copyFrom(getIdFromRecord(k).getBytes()))).get())
          .collect(Collectors.toSet());

      var valueMap = listOfPairs.stream().collect(Collectors.toMap(k -> k.getKey(), v -> v.getValue()));

      var keysToInsert = Sets.difference(keys, existingKeys);
      var keysToUpdate = Sets.difference(existingKeys, keysToInsert);

      LOG.info("Inserting {} rows and updating {} rows in {}", keysToInsert.size(), keysToUpdate.size(), table);

      txn.execute(
          String.format("insert into %s << %s >>", table,
              keysToInsert.stream().map(k -> "?").collect(Collectors.joining(","))),
          keysToInsert.stream().map(k -> makeRecord(k, valueMap.get(k))).collect(Collectors.toList()));

      final var updateQuery = String.format("update %s as o set o.%s = ? where o.%s = ?", table, HASH_FIELD, ID_FIELD);
      keysToUpdate.forEach(k -> txn.execute(updateQuery, makeStorableValue(valueMap.get(k)), makeStorableKey(k)));

    });
  }

  @Override
  public void sendEvent(final String topic, final String data) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendEvent(final List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }
}

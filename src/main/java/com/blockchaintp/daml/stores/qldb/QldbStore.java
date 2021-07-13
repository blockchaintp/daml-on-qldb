package com.blockchaintp.daml.stores.qldb;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Opaque;
import com.blockchaintp.daml.serviceinterface.TransactionLog;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.google.common.collect.Sets;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import software.amazon.qldb.Executor;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.Result;
import software.amazon.qldb.exceptions.QldbDriverException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class QldbStore implements TransactionLog<IonValue, IonStruct> {

  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(QldbStore.class);

  private final QldbDriver driver;
  private final String table;

  public QldbStore(QldbDriver driver,
                   String table) {
    this.driver = driver;
    this.table = table;
  }

  public static QldbStoreBuilder forDriver(QldbDriver driver) {
    return QldbStoreBuilder.forDriver(driver);
  }

  @Override
  public Optional<Value<IonStruct>> get(Key<IonValue> key) throws StoreReadException {
    LOG.info("get id={} in table={}", () -> key, () -> table);
    final var query = String.format("select o.* from %s AS o where o.id = ?", table);
    LOG.info("QUERY = {}", () -> query);

    try {
      final var r = driver.execute(
        (Executor<Result>) ex -> ex.execute(query,
          key.toNative()
        ));

      if (!r.iterator().hasNext()) {
        return Optional.empty();
      } else {
        return Optional.of(new Value<>(
          (IonStruct) r.iterator().next()
        ));
      }

    } catch (QldbDriverException e) {
      throw new StoreReadException("Driver error", e);
    }

  }

  @Override
  public Map<Key<IonValue>, Value<IonStruct>> get(List<Key<IonValue>> listOfKeys) throws StoreReadException {
    LOG.info("get ids=({}) in table={}", () -> listOfKeys, () -> table);

    final var query = String.format("select o.* from %s as o where o.id in ( %s )",
      table,
      listOfKeys.stream().map(k -> "?")
        .collect(Collectors.joining(","))
    );

    LOG.info("QUERY = {}", () -> query);

    try {
      final var r = driver.execute(
        // Invoke as varargs variant for stubbing reasons
        (Executor<Result>) ex -> ex.execute(query,
          listOfKeys.stream()
            .map(Opaque::toNative)
            .toArray(IonValue[]::new)
        ));

      //Pull id out of the struct to use for our result map
      return StreamSupport.stream(r.spliterator(), false)
        .map(IonStruct.class::cast)
        .collect(Collectors.toMap(
          k -> new Key<>(k.get("id")),
          Value::new
        ));
    } catch (QldbDriverException e) {
      throw new StoreReadException("Driver", e);
    }
  }


  /**
   * Put a single item to QLDB efficiently, conditionally and atomically using update or insert depending if the item exists
   *
   * @param key
   * @param value
   * @throws StoreWriteException
   */
  @Override
  public void put(Key<IonValue> key, Value<IonStruct> value) throws StoreWriteException {

    LOG.info("upsert id={} in table={}", () -> key, () -> table);

    driver.execute(tx -> {
      var exists = tx.execute(
        String.format("select o.id from %s as o where o.id = ?", table),
        key.toNative());

      if (exists.isEmpty()) {
        final var query = String.format("insert into %s value ?", table);
        tx.execute(query, value.toNative());
      } else {
        final var query = String.format("update %s as o set o = ? where o.id = ?", table);
        tx.execute(query, value.toNative(), key.toNative());
      }
    });
  }

  /**
   * Put multiple items to the store as efficiently as possible. We issue a select for all the keys, bulk insert those that are not present and update those that are.
   * There are potential issues with quotas @see <a href="https://docs.aws.amazon.com/qldb/latest/developerguide/limits.html">QLDB Quotas</a> :
   * TODO: Page by a configurable value that defaults to the current QLDB limit of 40 documents per transaction
   * TODO: (Harder) QLDB will complain if a transaction causes a modification of more than 4Mb of data, which cannot be determined up front, the whole transaction would need to be retried with a smaller set of documents, losing atomicity
   *
   * @param listOfPairs A key / value list of IonValues and IonStructs
   * @throws StoreWriteException
   */
  @Override
  public void put(List<Map.Entry<Key<IonValue>, Value<IonStruct>>> listOfPairs) throws StoreWriteException {
    LOG.debug("upsert ids={} in table={}", () -> listOfPairs
        .stream()
        .map(Map.Entry::getKey)
        .collect(Collectors.toList()),
      () -> table);

    driver.execute(txn -> {
      var keys = listOfPairs
        .stream()
        .map(k -> k.getKey().toNative())
        .collect(Collectors.toSet());

      var exists =
        StreamSupport.stream(
          txn.execute(
            String.format("select o.id from %s as o where o.id in ( %s )",
              table,
              keys
                .stream()
                .map(k -> "?")
                .collect(Collectors.joining(","))
            ),
            new ArrayList<>(keys)
          ).spliterator(), false)
          .collect(Collectors.toSet());

      // results are tuples of {id,value}
      var existingKeys = exists
        .stream()
        .map(IonStruct.class::cast)
        .map(k -> k.get("id"))
        .collect(Collectors.toSet());

      var valueMap = listOfPairs
        .stream()
        .collect(Collectors.toMap(
          k -> k.getKey().toNative(),
          v -> v.getValue().toNative()
        ));
      var keysToInsert = Sets.difference(keys, existingKeys);
      var keysToUpdate = Sets.difference(existingKeys, keysToInsert);

      LOG.info("Inserting {} rows and updating {} rows in {}",
        keysToInsert.size(),
        keysToUpdate.size(),
        table
      );

      txn.execute(
        String.format("insert into %s << %s >>",
          table,
          keysToInsert
            .stream()
            .map(k -> "?")
            .collect(Collectors.joining(","))
        ),
        keysToInsert
          .stream()
          .map(valueMap::get)
          .collect(Collectors.toList())
      );

      final var updateQuery = String.format("update %s as o set o = ? where o.id = ?", table);
      keysToUpdate.forEach(k ->
        txn.execute(updateQuery, valueMap.get(k), k)
      );

    });

  }

  @Override
  public void sendEvent(String topic, String data) throws StoreWriteException {

  }

  @Override
  public void sendEvent(List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException {

  }
}

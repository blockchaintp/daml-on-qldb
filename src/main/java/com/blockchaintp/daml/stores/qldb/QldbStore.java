package com.blockchaintp.daml.stores.qldb;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.TransactionLog;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.google.common.collect.Sets;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import software.amazon.qldb.Executor;
import software.amazon.qldb.ExecutorNoReturn;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.Result;
import software.amazon.qldb.exceptions.QldbDriverException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class QLDBStore implements TransactionLog {

  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(QLDBStore.class);

  private final QldbDriver driver;
  private final String table;
  private final IonSystem ion;
  private String ledger;

  public QLDBStore(QldbDriver driver,
                   String ledger,
                   String table,
                   IonSystem ion) {
    this.ledger = ledger;
    this.driver = driver;
    this.table = table;
    this.ion = ion;
  }

  public static QLDBStoreBuilder forDriver(QldbDriver driver) {
    return QLDBStoreBuilder.forDriver(driver);
  }

  public <K, V> Optional<Value<V>> get(Key<K> key, Class<V> valueClass) throws StoreReadException {
    LOG.info("get id={} in table={}", () -> key, () -> table);
    final var query = String.format("select o.* from %s AS o where o.id = ?", table);
    LOG.info("QUERY = {}", () -> query);

    try {
      final var r = driver.execute(
        (Executor<Result>) ex -> ex.execute(query,
          (IonValue) key.toNative()
        ));

      if (!r.iterator().hasNext()) {
        return Optional.empty();
      } else {
        return Optional.of(new Value((V) r.iterator().next()));
      }

    } catch (QldbDriverException e) {
      throw new StoreReadException("Driver error", e);
    }

  }

  @Override
  public <K, V> Map<Key<K>, Value<V>> get(List<Key<K>> listOfKeys, Class<V> valueclass) throws StoreReadException {
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
            .map(k -> (IonValue) k.toNative())
            .toArray(IonValue[]::new)
        ));

      //Pull id out of the struct to use for our result map
      return StreamSupport.stream(r.spliterator(), false)
        .map(IonStruct.class::cast)
        .collect(Collectors.toMap(
          k -> new Key(k.get("id")),
          v -> new Value(v)
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
   * @param <K>
   * @param <V>
   * @throws StoreWriteException
   */
  @Override
  public <K, V> void put(Key<K> key, Value<V> value) throws StoreWriteException {
    final var id = (IonValue) key.toNative();
    final var doc = (IonValue) value.toNative();

    LOG.info("upsert id={} in table={}", () -> id, () -> table);

    driver.execute(tx -> {
      var exists = tx.execute(
        String.format("select o.id from %s as o where o.id = ?", table),
        id);

      if (exists.isEmpty()) {
        final var query = String.format("insert into %s value ?", table);
        tx.execute(query, doc);
      } else {
        final var query = String.format("update %s as o set o = ? where o.id = ?", table);
        tx.execute(query, doc, id);
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
   * @param <K>
   * @param <V>
   * @throws StoreWriteException
   */
  @Override
  public <K, V> void put(List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
    LOG.debug("upsert ids={} in table={}", () -> listOfPairs
        .stream()
        .map(Map.Entry::getKey)
        .collect(Collectors.toList()),
      () -> table);

    driver.execute((ExecutorNoReturn) txn -> {
      var keys = listOfPairs
        .stream()
        .map(k -> (IonValue) k.getKey().toNative())
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
            keys.stream().collect(Collectors.toList())
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
          k -> (IonValue) k.getKey().toNative(),
          v -> (IonValue) v.getValue().toNative()
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
          .map(k -> valueMap.get(k))
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
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendEvent(List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }
}

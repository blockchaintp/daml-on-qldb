package com.blockchaintp.daml.stores.layers;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.Value;
import com.blockchaintp.daml.streams.StoreSpliterator;

/**
 * Stores an index of offset -> key when put operations complete and allows ordered streaming by offset.
 * @param <K>
 * @param <V>
 */
public class OffsetStore<K, V> implements Store<K, V> {

  private final Store<K, V> valueStore;
  private final Store<Long, K> offsetStore;

  /**
   * Constructs an ordered store from an underlying value store and an offset store.
   * @param theValueStore
   * @param theOffsetStore
   */
  public OffsetStore(final Store<K, V> theValueStore, final Store<Long, K> theOffsetStore) {
    this.valueStore = theValueStore;
    this.offsetStore = theOffsetStore;
  }

  @Override
  public final Optional<Value<V>> get(final Key<K> key) throws StoreReadException {
    return Optional.empty();
  }

  @Override
  public final Map<Key<K>, Value<V>> get(final List<Key<K>> listOfKeys) throws StoreReadException {
    return null;
  }

  @Override
  public final void put(final Key<K> key, final Value<V> value) throws StoreWriteException {

  }

  @Override
  public final void put(final List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {

  }

  /**
   *
   * @param offset
   * @return A stream of results starting at offset.
   */
  public final StoreSpliterator<K, V> stream(final long offset) {
    throw new UnsupportedOperationException();
  }
}

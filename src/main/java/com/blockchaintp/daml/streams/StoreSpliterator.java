package com.blockchaintp.daml.streams;

import java.util.Comparator;
import java.util.Map;
import java.util.Spliterators;
import java.util.function.Consumer;

import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.Value;

/**
 *
 * @param <K>
 * @param <V>
 */
public final class StoreSpliterator<K, V> extends Spliterators.AbstractSpliterator<Map.Entry<Key<K>, Value<V>>> {

  /**
   *
   * @param theStore
   */
  public StoreSpliterator(final Store<K, V> theStore) {
    super(Long.MAX_VALUE, 0);
  }

  @Override
  public boolean tryAdvance(final Consumer<? super Map.Entry<Key<K>, Value<V>>> action) {
    return false;
  }

  @Override
  public void forEachRemaining(final Consumer<? super Map.Entry<Key<K>, Value<V>>> action) {
    super.forEachRemaining(action);
  }

  @Override
  public long getExactSizeIfKnown() {
    return super.getExactSizeIfKnown();
  }

  @Override
  public boolean hasCharacteristics(final int characteristics) {
    return super.hasCharacteristics(characteristics);
  }

  @Override
  public Comparator<? super Map.Entry<Key<K>, Value<V>>> getComparator() {
    return super.getComparator();
  }
}

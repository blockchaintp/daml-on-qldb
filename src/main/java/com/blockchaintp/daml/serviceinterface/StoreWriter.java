package com.blockchaintp.daml.serviceinterface;

import java.util.List;
import java.util.Map;

import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;

/**
 * Represents a K/V store which we can write to.
 * @param <K> the type of the keys
 * @param <V> the type of the values
 */
public interface StoreWriter<K, V> {

  /**
   * Put the specified value at the specified key.
   * @param key the key
   * @param value the value
   * @throws StoreWriteException error writing to store
   */
  void put(Key<K> key, Value<V> value) throws StoreWriteException;

  /**
   * Put a list of K/V pairs t the store.
   * @param listOfPairs the list of K/V pairs to put
   * @throws StoreWriteException error writing to the store
   */
  void put(List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException;

}

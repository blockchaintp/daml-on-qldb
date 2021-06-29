package com.blockchaintp.daml.serviceinterface;

import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;

import java.util.List;
import java.util.Map;

/**
 *
 */
public interface StoreReader {
  <K, V> Value<V> get(Key<K> key) throws StoreReadException;

  <K, V> Map<Key<K>, Value<V>> get(List<Key<K>> listOfKeys) throws StoreReadException;
}

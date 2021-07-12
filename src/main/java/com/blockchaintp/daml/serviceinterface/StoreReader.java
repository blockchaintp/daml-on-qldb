package com.blockchaintp.daml.serviceinterface;

import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public interface StoreReader {
  <K, V> Optional<Value<V>> get(Key<K> key, Class<V> valueClass) throws StoreReadException;

  <K, V> Map<Key<K>, Value<V>> get(List<Key<K>> listOfKeys, Class<V> valueClass) throws StoreReadException;
}

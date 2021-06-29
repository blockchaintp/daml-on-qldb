package com.blockchaintp.daml.serviceinterface;

import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;

import java.util.List;
import java.util.Map;

public interface StoreWriter {
  <K, V> void put(Key<K> key, Value<V> value) throws StoreWriteException;

  <K, V> void put(List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException;

  void sendEvent(String topic, String data) throws StoreWriteException;

  void sendEvent(List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException;
}

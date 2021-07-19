package com.blockchaintp.daml.stores;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

public class StubStore<K, V> implements TransactionLog<K, V> {
  private final Map<Key<K>, Value<V>> stored = new HashMap<>();

  @Override
  public final Optional<Value<V>> get(final Key<K> key) throws StoreReadException {
    return Optional.ofNullable(stored.get(key));
  }

  @Override
  public final Map<Key<K>, Value<V>> get(final List<Key<K>> listOfKeys) throws StoreReadException {
    return listOfKeys.stream().filter(stored::containsKey)
      .collect(Collectors.toMap(k -> k, stored::get, (a, b) -> b, HashMap::new));
  }

  @Override
  public final void put(final Key<K> key, final Value<V> value) throws StoreWriteException {
    stored.put(key, value);
  }

  @Override
  public final void put(final List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
    listOfPairs.forEach(kv -> stored.put(kv.getKey(), kv.getValue()));
  }

  @Override
  public void sendEvent(List<Entry<String, String>> listOfTopicDataPairs) throws StoreWriteException {
    // TODO Auto-generated method stub
  }

  @Override
  public void sendEvent(String topic, String data) throws StoreWriteException {
    // TODO Auto-generated method stub
  }
}

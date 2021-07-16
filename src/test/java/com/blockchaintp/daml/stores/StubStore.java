package com.blockchaintp.daml.stores;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Store;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;

public class StubStore<K, V> implements Store<K, V> {
  final Map<Key<K>, Value<V>> stored = new HashMap<>();

  @Override
  public Optional<Value<V>> get(Key<K> key) throws StoreReadException {
    return Optional.ofNullable(stored.get(key));
  }

  @Override
  public Map<Key<K>, Value<V>> get(List<Key<K>> listOfKeys) throws StoreReadException {
    return
      listOfKeys
        .stream()
        .filter(stored::containsKey)
        .collect(Collectors.toMap(k -> k, stored::get, (a, b) -> b, HashMap::new));
  }

  @Override
  public void put(Key<K> key, Value<V> value) throws StoreWriteException {
    stored.put(key, value);
  }

  @Override
  public void put(List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
    listOfPairs.forEach(kv -> stored.put(kv.getKey(), kv.getValue()));
  }

}

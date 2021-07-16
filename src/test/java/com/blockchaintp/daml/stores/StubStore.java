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

}

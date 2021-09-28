/*
 * Copyright 2021 Blockchain Technology Partners
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.blockchaintp.daml.stores;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class StubStore<K, V> implements Store<K, V> {
  private final ConcurrentMap<Key<K>, Value<V>> stored = new ConcurrentHashMap<>();

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
  public final void put(final List<Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
    listOfPairs.forEach(kv -> stored.put(kv.getKey(), kv.getValue()));
  }
}

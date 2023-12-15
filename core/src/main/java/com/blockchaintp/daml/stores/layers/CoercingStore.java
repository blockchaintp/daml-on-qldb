/*
 * Copyright © 2023 Paravela Limited
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
package com.blockchaintp.daml.stores.layers;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.Value;

/**
 * A store that will coerce key and value types of an underlying store using supplied bijections.
 *
 * @param <K1>
 *          The target key type.
 * @param <K2>
 *          The underlying key type.
 * @param <V1>
 *          The target value type.
 * @param <V2>
 *          The underlying value type.
 */
public class CoercingStore<K1, K2, V1, V2> implements Store<K1, V1> {
  private final Store<K2, V2> inner;
  private final Bijection<K1, K2> keyCoercion;
  private final Bijection<V1, V2> valueCoercion;

  /**
   * Convenience method to wrap a store.
   *
   * @param theKeyCoercion
   * @param theValueCoercion
   * @param inner
   * @param <K3>
   * @param <K4>
   * @param <V3>
   * @param <V4>
   * @return a wrapped store
   */
  public static <K3, K4, V3, V4> Store<K3, V3> from(final Bijection<K3, K4> theKeyCoercion,
      final Bijection<V3, V4> theValueCoercion, final Store<K4, V4> inner) {
    return new CoercingStore<>(theKeyCoercion, theValueCoercion, inner);
  }

  /**
   * Wrap an underlying store with value and kehy coercions.
   *
   * @param theInner
   * @param theKeyCoercion
   * @param theValueCoercion
   */
  public CoercingStore(final Bijection<K1, K2> theKeyCoercion, final Bijection<V1, V2> theValueCoercion,
      final Store<K2, V2> theInner) {
    this.inner = theInner;
    keyCoercion = theKeyCoercion;
    valueCoercion = theValueCoercion;
  }

  @Override
  public final Optional<Value<V1>> get(final Key<K1> key) throws StoreReadException {
    return inner.get(key.map(keyCoercion::to)).map(o -> o.map(valueCoercion::from));
  }

  @Override
  public final Map<Key<K1>, Value<V1>> get(final List<Key<K1>> listOfKeys) throws StoreReadException {
    return inner.get(listOfKeys.stream().map(k -> k.map(keyCoercion::to)).collect(Collectors.toList())).entrySet()
        .stream().collect(
            Collectors.toMap(kv -> kv.getKey().map(keyCoercion::from), kv -> kv.getValue().map(valueCoercion::from)));
  }

  @Override
  public final void put(final Key<K1> key, final Value<V1> value) throws StoreWriteException {
    inner.put(key.map(keyCoercion::to), value.map(valueCoercion::to));
  }

  @Override
  public final void put(final List<Map.Entry<Key<K1>, Value<V1>>> listOfPairs) throws StoreWriteException {
    inner.put(listOfPairs.stream()
        .map(kv -> Map.entry(kv.getKey().map(keyCoercion::to), kv.getValue().map(valueCoercion::to)))
        .collect(Collectors.toList()));
  }
}

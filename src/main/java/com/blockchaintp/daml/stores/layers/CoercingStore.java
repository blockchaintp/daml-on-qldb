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
package com.blockchaintp.daml.stores.layers;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
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
  private final Function<K2, K1> keyCoercionFrom;
  private final Function<V2, V1> valueCoercionFrom;
  private final Function<K1, K2> keyCoercionTo;
  private final Function<V1, V2> valueCoercionTo;

  /**
   * Convenience method to wrap a store.
   *
   * @param keyCoercionFrom
   * @param valueCoercionFrom
   * @param keyCoercionTo
   * @param valueCoercionTo
   * @param inner
   * @param <K3>
   * @param <K4>
   * @param <V3>
   * @param <V4>
   * @return a wrapped store
   */
  public static <K3, K4, V3, V4> Store<K3, V3> from(final Function<K4, K3> keyCoercionFrom,
      final Function<V4, V3> valueCoercionFrom, final Function<K3, K4> keyCoercionTo,
      final Function<V3, V4> valueCoercionTo, final Store<K4, V4> inner) {
    return new CoercingStore<K3, K4, V3, V4>(keyCoercionFrom, keyCoercionTo, valueCoercionFrom, valueCoercionTo, inner);
  }

  /**
   * Wrap an underlying store with value and kehy coercions.
   *
   * @param theInner
   * @param theKeyCoercionFrom
   * @param theValueCoercionFrom
   * @param theKeyCoercionTo
   * @param theValueCoercionTo
   */
  public CoercingStore(final Function<K2, K1> theKeyCoercionFrom, final Function<K1, K2> theKeyCoercionTo,
      final Function<V2, V1> theValueCoercionFrom, final Function<V1, V2> theValueCoercionTo,
      final Store<K2, V2> theInner) {
    this.inner = theInner;
    this.keyCoercionFrom = theKeyCoercionFrom;
    this.valueCoercionFrom = theValueCoercionFrom;
    this.keyCoercionTo = theKeyCoercionTo;
    this.valueCoercionTo = theValueCoercionTo;
  }

  @Override
  public final Optional<Value<V1>> get(final Key<K1> key) throws StoreReadException {
    return inner.get(key.map(keyCoercionTo)).map(o -> o.map(valueCoercionFrom));
  }

  @Override
  public final Map<Key<K1>, Value<V1>> get(final List<Key<K1>> listOfKeys) throws StoreReadException {
    return inner.get(listOfKeys.stream().map(k -> k.map(keyCoercionTo)).collect(Collectors.toList())).entrySet()
        .stream()
        .collect(Collectors.toMap(kv -> kv.getKey().map(keyCoercionFrom), kv -> kv.getValue().map(valueCoercionFrom)));
  }

  @Override
  public final void put(final Key<K1> key, final Value<V1> value) throws StoreWriteException {
    inner.put(key.map(keyCoercionTo), value.map(valueCoercionTo));
  }

  @Override
  public final void put(final List<Map.Entry<Key<K1>, Value<V1>>> listOfPairs) throws StoreWriteException {
    inner.put(
        listOfPairs.stream().map(kv -> Map.entry(kv.getKey().map(keyCoercionTo), kv.getValue().map(valueCoercionTo)))
            .collect(Collectors.toList()));
  }
}

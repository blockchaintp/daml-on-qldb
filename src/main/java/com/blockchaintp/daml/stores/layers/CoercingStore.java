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
 * @param <K1> The target key type.
 * @param <K2> The underlying key type.
 * @param <V1> The target value type.
 * @param <V2> The underlying value type.
 */
public class CoercingStore<K1, K2, V1, V2> implements Store<K1, V1> {
  private final Store<K2, V2> inner;
  private final Function<K2, K1> keyCoercionFrom;
  private final Function<V2, V1> valueCoercionFrom;
  private final Function<K1, K2> keyCoercionTo;
  private final Function<V1, V2> valueCoercionTo;


  /**
   * Wrap an underlying store with value and kehy coercions.
   * @param theInner
   * @param theKeyCoercionFrom
   * @param theValueCoercionFrom
   * @param theKeyCoercionTo
   * @param theValueCoercionTo
   */
  public CoercingStore(final Store<K2, V2> theInner, final Function<K2, K1> theKeyCoercionFrom, final Function<V2, V1> theValueCoercionFrom, final Function<K1, K2> theKeyCoercionTo, final Function<V1, V2> theValueCoercionTo) {
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
    return inner.get(listOfKeys.stream().map(k -> k.map(keyCoercionTo)).collect(Collectors.toList()))
      .entrySet()
      .stream()
      .collect(Collectors.toMap(kv -> kv.getKey().map(keyCoercionFrom),
        kv -> kv.getValue().map(valueCoercionFrom)
      ));
  }

  @Override
  public final void put(final Key<K1> key, final Value<V1> value) throws StoreWriteException {
    inner.put(key.map(keyCoercionTo), value.map(valueCoercionTo));
  }

  @Override
  public final void put(final List<Map.Entry<Key<K1>, Value<V1>>> listOfPairs) throws StoreWriteException {
    inner.put(listOfPairs.stream().map(
      kv -> Map.entry(kv.getKey().map(keyCoercionTo), kv.getValue().map(valueCoercionTo))
    ).collect(Collectors.toList()));
  }
}

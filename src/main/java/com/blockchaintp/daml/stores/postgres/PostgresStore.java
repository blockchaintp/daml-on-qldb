package com.blockchaintp.daml.stores.postgres;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.Value;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A PostgresStore is a store backed by a postgres interface.
 */
public class PostgresStore implements Store<String, byte[]> {
  @Override
  public final Optional<Value<byte[]>> get(final Key<String> key) throws StoreReadException {
    return Optional.empty();
  }

  @Override
  public final Map<Key<String>, Value<byte[]>> get(final List<Key<String>> listOfKeys) {
    return null;
  }

  @Override
  public final void put(final Key<String> key, final Value<byte[]> value) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void put(final List<Map.Entry<Key<String>, Value<byte[]>>> listOfPairs) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

}

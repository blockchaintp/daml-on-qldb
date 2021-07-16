package com.blockchaintp.daml.stores.postgres;

import com.blockchaintp.daml.serviceinterface.Store;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;

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

package com.blockchaintp.daml.stores.postgres;

import com.blockchaintp.daml.serviceinterface.BlobStore;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PostgresStore implements BlobStore<String, byte[]> {
  @Override
  public Optional<Value<byte[]>> get(Key<String> key) throws StoreReadException {
    return Optional.empty();
  }

  @Override
  public Map<Key<String>, Value<byte[]>> get(List<Key<String>> listOfKeys) {
    return null;
  }

  @Override
  public void put(Key<String> key, Value<byte[]> value) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void put(List<Map.Entry<Key<String>, Value<byte[]>>> listOfPairs) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendEvent(String topic, String data) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendEvent(List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }
}

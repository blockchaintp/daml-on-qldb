package com.blockchaintp.daml.stores.qldbblobstore;

import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.TransactionLog;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class QLDBBlobStore implements TransactionLog {

  @Override
  public <K, V> void put(Key<K> key, Value<V> value) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> void put(List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
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

  @Override
  public <K, V> Optional<Value<V>> get(Key<K> key, Class<V> valueClass) throws StoreReadException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> Map<Key<K>, Value<V>> get(List<Key<K>> listOfKeys, Class<V> valueClass) throws StoreReadException {
    throw new UnsupportedOperationException();
  }
}

package com.blockchaintp.daml.serviceinterface;

public interface Store<K, V>
  extends StoreReader<K, V>, StoreWriter<K, V> {
}

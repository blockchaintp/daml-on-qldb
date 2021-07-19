package com.blockchaintp.daml.stores.service;

/**
 * A Store is a combined StoreReader and StoreWriter representing a K/V store.
 * @param <K> the type of the keys
 * @param <V> the type of the values
 */
public interface Store<K, V>
  extends StoreReader<K, V>, StoreWriter<K, V> {
}

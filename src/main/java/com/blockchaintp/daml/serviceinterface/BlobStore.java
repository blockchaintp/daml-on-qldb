package com.blockchaintp.daml.serviceinterface;

public interface BlobStore<K, V>
  extends Store<K, V>,
  BlobStoreReader<K, V>,
  BlobStoreWriter<K, V> {
}

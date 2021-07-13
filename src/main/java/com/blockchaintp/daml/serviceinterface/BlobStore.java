package com.blockchaintp.daml.serviceinterface;

public interface BlobStore<K, V>
  extends BlobStoreReader<K, V>, BlobStoreWriter<K, V> {
}

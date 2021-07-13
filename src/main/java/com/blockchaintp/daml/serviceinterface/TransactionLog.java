package com.blockchaintp.daml.serviceinterface;

public interface TransactionLog<K, V> extends
  Store<K, V>,
  TransactionLogReader<K, V>,
  TransactionLogWriter<K, V> {
}

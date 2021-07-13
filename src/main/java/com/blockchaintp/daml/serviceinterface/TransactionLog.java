package com.blockchaintp.daml.serviceinterface;

public interface TransactionLog<K, V> extends
  TransactionLogReader<K, V>,
  TransactionLogWriter<K, V> {
}

package com.blockchaintp.daml.serviceinterface;

/**
 * A TransactionLog is a logging K/V store, i.e. it also has the ability to record events.
 * @param <K> the type of the keys
 * @param <V> the type of the values
 */
public interface TransactionLog<K, V> extends
  Store<K, V>,
  TransactionLogWriter<K, V> {
}

/*
 * Copyright 2021 Blockchain Technology Partners
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.blockchaintp.daml.participant;

import java.util.UUID;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.address.LedgerAddress;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.daml.lf.engine.Engine;
import com.daml.metrics.Metrics;
import com.daml.platform.akkastreams.dispatcher.Dispatcher;
import com.google.protobuf.ByteString;

/**
 *
 * @param <I>
 * @param <A>
 */
public final class InProcLedgerSubmitterBuilder<I extends Identifier, A extends LedgerAddress> {
  private Store<ByteString, ByteString> stateStore;
  private TransactionLog<UUID, ByteString, Long> txLog;
  private Engine engine;
  private Metrics metrics;
  private Dispatcher<Long> dispatcher;
  private static final int MAX_CONCURRENT = 20;
  private int maxConcurrent = MAX_CONCURRENT;
  private static final int SLOW_CALL_DURATION = 10000;
  private long slowCallDuration = SLOW_CALL_DURATION;

  /**
   *
   * @param theStateStore
   * @return A configured builder.
   */
  public InProcLedgerSubmitterBuilder<I, A> withStateStore(final Store<ByteString, ByteString> theStateStore) {
    stateStore = theStateStore;

    return this;
  }

  /**
   *
   * @param theTxLog
   * @return A configured builder.
   */
  public InProcLedgerSubmitterBuilder<I, A> withTransactionLogWriter(
      final TransactionLog<UUID, ByteString, Long> theTxLog) {
    txLog = theTxLog;

    return this;
  }

  /**
   *
   * @param theEngine
   * @return A configured builder.
   */
  public InProcLedgerSubmitterBuilder<I, A> withEngine(final Engine theEngine) {
    engine = theEngine;
    return this;
  }

  /**
   *
   * @param theMetrics
   * @return A configured builder
   */
  public InProcLedgerSubmitterBuilder<I, A> withMetrics(final Metrics theMetrics) {
    metrics = theMetrics;
    return this;
  }

  /**
   * Slow call duration, concurrent calls over this time will open the circuit breaker and cause back
   * pressure.
   *
   * @param duration
   * @return A configured builder.
   */
  public InProcLedgerSubmitterBuilder<I, A> withSlowCall(final int duration) {
    slowCallDuration = duration;

    return this;
  }

  /**
   * The maximum number of concurrently executing ledger submissions before backpressure is applied.
   *
   * @param max
   * @return A configured builder.
   */
  public InProcLedgerSubmitterBuilder<I, A> withMaxThroughput(final int max) {
    maxConcurrent = max;
    return this;
  }

  /**
   *
   * @param theDispatcher
   * @return A configured t builder.
   */
  public InProcLedgerSubmitterBuilder<I, A> withDispatcher(final Dispatcher<Long> theDispatcher) {
    dispatcher = theDispatcher;

    return this;
  }

  /**
   *
   * @return A configured ledger sumbitter.
   */
  public LedgerSubmitter<I, A> build() {
    var inproc = new InProcLedgerSubmitter<I, A>(engine, metrics, txLog, stateStore, dispatcher);
    return new LedgerSubmitterBulkhead<>(inproc, maxConcurrent, slowCallDuration);
  }

}

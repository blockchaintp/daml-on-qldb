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
import com.daml.ledger.participant.state.v1.Configuration;
import com.daml.lf.engine.Engine;
import com.daml.logging.LoggingContext;
import com.daml.metrics.Metrics;
import com.google.protobuf.ByteString;

import scala.concurrent.ExecutionContext;

/**
 *
 * @param <I>
 * @param <A>
 */
public final class InProcLedgerSubmitterBuilder<I extends Identifier, A extends LedgerAddress> {
  private String participantId;
  private ExecutionContext executionContext;
  private Store<ByteString, ByteString> stateStore;
  private TransactionLog<UUID, ByteString, Long> txLog;
  private Engine engine;
  private Metrics metrics;
  private LoggingContext loggingContext;
  private Configuration configuration;

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
   * @param theParticipantId
   * @return A configured builder.
   */
  public InProcLedgerSubmitterBuilder<I, A> withParticipantId(final String theParticipantId) {
    participantId = theParticipantId;

    return this;
  }

  /**
   *
   * @param theExecutionContext
   * @return A conffigured builder.
   */
  public InProcLedgerSubmitterBuilder<I, A> withExecutionContext(final ExecutionContext theExecutionContext) {
    executionContext = theExecutionContext;

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
   *
   * @param theLoggingContext
   * @return A configured builder.
   */
  public InProcLedgerSubmitterBuilder<I, A> withLoggingContext(final LoggingContext theLoggingContext) {
    loggingContext = theLoggingContext;
    return this;
  }

  /**
   *
   * @param theConfiguration
   * @return A configured builder.
   */
  public InProcLedgerSubmitterBuilder<I, A> withConfiguration(final Configuration theConfiguration) {
    configuration = theConfiguration;
    return this;
  }

  /**
   *
   * @return A configured ledger sumbitter.
   */
  public InProcLedgerSubmitter<I, A> build() {
    return new InProcLedgerSubmitter<I, A>(engine, metrics, txLog, stateStore, participantId, configuration,
        loggingContext);
  }
}

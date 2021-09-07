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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.address.LedgerAddress;
import com.blockchaintp.daml.stores.LRUCache;
import com.blockchaintp.daml.stores.layers.Bijection;
import com.blockchaintp.daml.stores.layers.CoercingStore;
import com.blockchaintp.daml.stores.layers.CoercingTxLog;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.TransactionLogWriter;
import com.blockchaintp.utility.Functions;
import com.blockchaintp.utility.UuidConverter;
import com.daml.api.util.TimeProvider;
import com.daml.ledger.participant.state.kvutils.DamlKvutils;
import com.daml.ledger.participant.state.kvutils.Raw;
import com.daml.ledger.participant.state.v1.SubmissionResult;
import com.daml.ledger.validator.SubmissionValidator;
import com.daml.ledger.validator.ValidatingCommitter;
import com.daml.lf.engine.Engine;
import com.daml.metrics.Metrics;
import com.daml.platform.akkastreams.dispatcher.Dispatcher;
import com.google.protobuf.ByteString;

import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import scala.concurrent.Await$;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.Duration;
import scala.runtime.BoxedUnit;

/**
 * An in process submitter relying on an ephemeral queue.
 *
 * @param <A>
 * @param <B>
 */
public final class InProcLedgerSubmitter<A extends Identifier, B extends LedgerAddress>
    implements LedgerSubmitter<A, B> {

  private static final int STATE_CACHE_SIZE = 1000;
  private final ValidatingCommitter<Long> comitter;

  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(InProcLedgerSubmitter.class);
  private final Dispatcher<Long> dispatcher;
  private final TransactionLogWriter<Raw.LogEntryId, Raw.Envelope, Long> writer;
  private final ExecutionContext context;

  /**
   *
   * @param <I>
   * @param <A>
   * @return An appropriate InProcLedgerSubmitterBuilder.
   */
  public static <I extends Identifier, A extends LedgerAddress> InProcLedgerSubmitterBuilder<I, A> builder() {
    return new InProcLedgerSubmitterBuilder<>();
  }

  /**
   * This seems like a terrible way to go about things that are meant to be bytewise equivalent?
   *
   * @param id
   * @return A daml log entry id parsed from a daml log entry id.
   */
  private DamlKvutils.DamlLogEntryId logEntryIdToDamlLogEntryId(final Raw.LogEntryId id) {
    var parsed = Functions.uncheckFn(() -> DamlKvutils.DamlLogEntryId.parseFrom(id.bytes())).apply();

    LOG.info("parse log id {}", () -> parsed.getEntryId().toString());

    return parsed;
  }

  /**
   * @param theEngine
   * @param theMetrics
   * @param theTxLog
   * @param theStateStore
   * @param theDispatcher
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public InProcLedgerSubmitter(final Engine theEngine, final Metrics theMetrics,
      final TransactionLog<UUID, ByteString, Long> theTxLog, final Store<ByteString, ByteString> theStateStore,
      final Dispatcher<Long> theDispatcher) {
    writer = CoercingTxLog.from(Bijection.of(UuidConverter::logEntryToUuid, UuidConverter::uuidtoLogEntry),
        Bijection.of(Raw.Envelope::bytes, Raw.Envelope$.MODULE$::apply), Bijection.identity(), theTxLog);
    dispatcher = theDispatcher;

    comitter = new ValidatingCommitter<>(TimeProvider.UTC$.MODULE$::getCurrentTime,
        SubmissionValidator.create(
            new StateAccess(CoercingStore.from(Bijection.of(Raw.StateKey::bytes, Raw.StateKey$.MODULE$::apply),
                Bijection.of(Raw.Envelope::bytes, Raw.Envelope$.MODULE$::apply), theStateStore), writer),
            () -> logEntryIdToDamlLogEntryId(Functions.uncheckFn(writer::begin).apply()), false,
            new StateCache<>(new LRUCache<>(STATE_CACHE_SIZE)), theEngine, theMetrics),
        r -> {
          LOG.info("Signal new head {}", () -> r + 1);
          dispatcher.signalNewHead(r + 1);
          return BoxedUnit.UNIT;
        });

    context = scala.concurrent.ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor());
  }

  @Override
  public CompletableFuture<SubmissionStatus> submitPayload(final CommitPayload<A> cp) {

    return CompletableFuture.supplyAsync(() -> {
      SubmissionResult res = null;
      try {
        res = Await$.MODULE$.result(
            this.comitter.commit(cp.getCorrelationId(), cp.getSubmission(), cp.getSubmittingParticipantId(), context),
            Duration.Inf());
      } catch (InterruptedException theE) {
        return SubmissionStatus.PARTIALLY_SUBMITTED;
      } catch (TimeoutException theE) {
        return SubmissionStatus.PARTIALLY_SUBMITTED;
      }

      if (!(res instanceof SubmissionResult.Acknowledged$)) {
        return SubmissionStatus.REJECTED;
      } else {
        return SubmissionStatus.SUBMITTED;
      }
    });

  }
}

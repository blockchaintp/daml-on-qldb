/*
 * Copyright 2021-2022 Blockchain Technology Partners
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
import com.daml.ledger.participant.state.kvutils.Raw;
import com.daml.ledger.participant.state.kvutils.store.DamlLogEntryId;
import com.daml.ledger.participant.state.v2.SubmissionResult;
import com.daml.ledger.validator.SubmissionValidator$;
import com.daml.ledger.validator.ValidatingCommitter;
import com.daml.lf.engine.Engine;
import com.daml.metrics.Metrics;
import com.daml.platform.akkastreams.dispatcher.Dispatcher;
import com.google.protobuf.ByteString;

import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import scala.compat.java8.FutureConverters;
import scala.concurrent.ExecutionContextExecutor;
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
  private final ExecutionContextExecutor context;
  private Long highWaterMark = 0L;

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
  private DamlLogEntryId logEntryIdToDamlLogEntryId(final Raw.LogEntryId id) {
    var parsed = Functions.uncheckFn(() -> DamlLogEntryId.parseFrom(id.bytes())).apply();

    LOG.info("parse log id {}", () -> parsed.getEntryId().toString());

    return parsed;
  }

  /**
   * @param theEngine
   * @param theMetrics
   * @param theTxLog
   * @param theStateStore
   * @param theDispatcher
   * @param theContext
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public InProcLedgerSubmitter(final Engine theEngine, final Metrics theMetrics,
      final TransactionLog<UUID, ByteString, Long> theTxLog, final Store<ByteString, ByteString> theStateStore,
      final Dispatcher<Long> theDispatcher, final ExecutionContextExecutor theContext) {
    writer = CoercingTxLog.from(Bijection.of(UuidConverter::logEntryToUuid, UuidConverter::uuidtoLogEntry),
        Bijection.of(Raw.Envelope::bytes, Raw.Envelope$.MODULE$::apply), Bijection.identity(), theTxLog);
    dispatcher = theDispatcher;

    comitter = new ValidatingCommitter<>(TimeProvider.UTC$.MODULE$::getCurrentTime,
        SubmissionValidator$.MODULE$.createForTimeMode(
            new StateAccess(CoercingStore.from(Bijection.of(Raw.StateKey::bytes, Raw.StateKey$.MODULE$::apply),
                Bijection.of(Raw.Envelope::bytes, Raw.Envelope$.MODULE$::apply), theStateStore), writer),
            () -> logEntryIdToDamlLogEntryId(UuidConverter.uuidtoLogEntry(UUID.randomUUID())), false,
            new StateCache<>(new LRUCache<>(STATE_CACHE_SIZE)), theEngine, theMetrics),
        r -> {
          /// New head should be end of sequence, i.e. one past the actual head. This should really have a
          /// nicer type, we ensure it is monotonic here
          var head = r + 1;

          if (highWaterMark < head) {
            LOG.info("Signal new head {}", () -> head);
            dispatcher.signalNewHead(head);
            highWaterMark = head;
          }
          return BoxedUnit.UNIT;
        });

    context = theContext;
  }

  @Override
  public CompletableFuture<SubmissionStatus> submitPayload(final CommitPayload<A> cp) {
    return FutureConverters
        .toJava(
            this.comitter.commit(cp.getCorrelationId(), cp.getSubmission(), cp.getSubmittingParticipantId(), context))
        .thenApply(x -> {
          if (x == SubmissionResult.Acknowledged$.MODULE$) {
            return SubmissionStatus.SUBMITTED;
          }
          if (x instanceof SubmissionResult.SynchronousError) {
            return SubmissionStatus.REJECTED;
          }
          LOG.info("Overloaded {} {} ", cp.getCorrelationId(), x);
          return SubmissionStatus.OVERLOADED;
        }).toCompletableFuture();
  }

  @Override
  public CommitPayload<B> translatePayload(final CommitPayload<A> cp) {
    throw new UnsupportedOperationException();
  }
}

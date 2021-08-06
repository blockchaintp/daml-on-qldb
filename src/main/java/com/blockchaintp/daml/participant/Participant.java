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

import java.util.stream.Collectors;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.address.LedgerAddress;
import com.blockchaintp.daml.stores.service.TransactionLogReader;
import com.daml.ledger.api.health.HealthStatus;
import com.daml.ledger.participant.state.kvutils.Raw;
import com.daml.ledger.participant.state.kvutils.api.CommitMetadata;
import com.daml.ledger.participant.state.kvutils.api.LedgerReader;
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord;
import com.daml.ledger.participant.state.kvutils.api.LedgerWriter;
import com.daml.ledger.participant.state.v1.Offset;
import com.daml.ledger.participant.state.v1.SubmissionResult;
import com.daml.ledger.resources.ResourceContext;
import com.daml.lf.engine.Engine;
import com.daml.telemetry.TelemetryContext;

import akka.NotUsed;
import akka.stream.scaladsl.Source;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import scala.Option;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.jdk.javaapi.OptionConverters;

/**
 *
 * @param <I>
 * @param <A>
 */
public final class Participant<I extends Identifier, A extends LedgerAddress> implements LedgerReader, LedgerWriter {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(Participant.class);
  private final TransactionLogReader<Offset, Raw.LogEntryId, Raw.Envelope> txLog;
  private final CommitPayloadBuilder<I> commitPayloadBuilder;
  private final LedgerSubmitter<I, A> submitter;
  private final String ledgerId;
  private final String participantId;
  private final ExecutionContext context;

  /**
   * Convenience method for creating a builder.
   *
   * @param theEngine
   * @param theParticipantId
   * @param theLedgerId
   * @param theContext
   * @param <I2>
   * @param <A2>
   * @return A partially configured participant builder.
   */
  public static <I2 extends Identifier, A2 extends LedgerAddress> ParticipantBuilder<I2, A2> builder(
      final Engine theEngine, final String theParticipantId, final String theLedgerId,
      final ResourceContext theContext) {
    return new ParticipantBuilder<I2, A2>(theEngine, theParticipantId, theLedgerId, theContext);
  }

  /**
   * @param theTxLog
   * @param theCommitPayloadBuilder
   * @param theSubmitter
   * @param theLedgerId
   * @param theParticipantId
   * @param theContext
   */
  public Participant(final TransactionLogReader<Offset, Raw.LogEntryId, Raw.Envelope> theTxLog,
      final CommitPayloadBuilder<I> theCommitPayloadBuilder, final LedgerSubmitter<I, A> theSubmitter,
      final String theLedgerId, final String theParticipantId, final ExecutionContext theContext) {
    txLog = theTxLog;
    commitPayloadBuilder = theCommitPayloadBuilder;
    submitter = theSubmitter;
    ledgerId = theLedgerId;
    participantId = theParticipantId;
    context = theContext;
  }

  @Override
  public HealthStatus currentHealth() {
    return HealthStatus.healthy();
  }

  @Override
  public Source<LedgerRecord, NotUsed> events(final Option<Offset> startExclusive) {
    return Source.fromPublisher(txLog.from(OptionConverters.toJava(startExclusive))
        .map(rx -> LedgerRecord.apply(rx._1, rx._2, rx._3)).toFlowable(BackpressureStrategy.BUFFER));
  }

  @Override
  public String ledgerId() {
    return ledgerId;
  }

  @Override
  public String participantId() {
    return participantId;
  }

  @Override
  public Future<SubmissionResult> commit(final String correlationId, final Raw.Envelope envelope,
      final CommitMetadata metadata, final TelemetryContext telemetryContext) {

    return Future.apply(() -> {
      try {
        var ref = commitPayloadBuilder.build(envelope, metadata, correlationId).stream().map(submitter::submitPayload)
            .collect(Collectors.toList());
        return SubmissionResult.Acknowledged$.MODULE$;
      } catch (final Exception e) {
        LOG.warn("Interrupted while submitting transaction", e);
        Thread.currentThread().interrupt();
        return new SubmissionResult.InternalError("Interrupted while submitting transaction");
      }
    }, ExecutionContext.global());
  }
}

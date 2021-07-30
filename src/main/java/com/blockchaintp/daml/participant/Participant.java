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
import com.daml.telemetry.TelemetryContext;

import akka.NotUsed;
import akka.stream.scaladsl.Source;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;

import scala.Option;
import scala.concurrent.ExecutionContext;
import scala.jdk.javaapi.OptionConverters;
import scala.concurrent.Future;


/**
 *
 * @param <I>
 * @param <A>
 */
public final class Participant<I extends Identifier, A extends LedgerAddress> implements LedgerReader, LedgerWriter {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(Participant.class);
  private final TransactionLogReader<Offset, I, LedgerRecord> txLog;
  private final CommitPayloadBuilder<I> commitPayloadBuilder;
  private final LedgerSubmitter<I, A> submitter;
  private final String ledgerId;
  private final String participantId;

  /**
   *
   * @param theTxLog
   * @param theCommitPayloadBuilder
   * @param theSubmitter
   * @param theLedgerId
   * @param theParticipantId
   */
  public Participant(final TransactionLogReader<Offset, I, LedgerRecord> theTxLog, final CommitPayloadBuilder<I> theCommitPayloadBuilder, final LedgerSubmitter<I, A> theSubmitter, final String theLedgerId, final String theParticipantId) {
    txLog = theTxLog;
    commitPayloadBuilder = theCommitPayloadBuilder;
    submitter = theSubmitter;
    ledgerId = theLedgerId;
    participantId = theParticipantId;
  }

  @Override
  public HealthStatus currentHealth() {
    return HealthStatus.healthy();
  }

  @Override
  public Source<LedgerRecord, NotUsed> events(final Option<Offset> startExclusive) {
    return Source.fromPublisher(
      txLog.from(OptionConverters.toJava(startExclusive))
        .map(rx -> rx.getValue())
        .toFlowable(BackpressureStrategy.BUFFER)
    );
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
  public Future<SubmissionResult> commit(final String correlationId, final Raw.Envelope envelope, final CommitMetadata metadata, final TelemetryContext telemetryContext) {

    return Future.apply(() -> {
      try {
        var ref = commitPayloadBuilder.build(envelope, metadata, correlationId)
          .stream()
          .map(submitter::submitPayload)
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

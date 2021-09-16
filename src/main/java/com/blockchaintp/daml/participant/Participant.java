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

import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Collectors;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.address.LedgerAddress;
import com.blockchaintp.daml.stores.service.TransactionLogReader;
import com.daml.ledger.api.health.HealthStatus;
import com.daml.ledger.participant.state.kvutils.OffsetBuilder;
import com.daml.ledger.participant.state.kvutils.Raw;
import com.daml.ledger.participant.state.kvutils.api.CommitMetadata;
import com.daml.ledger.participant.state.kvutils.api.LedgerReader;
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord;
import com.daml.ledger.participant.state.kvutils.api.LedgerWriter;
import com.daml.ledger.participant.state.v1.Offset;
import com.daml.ledger.participant.state.v1.SubmissionResult;
import com.daml.ledger.resources.ResourceContext;
import com.daml.platform.akkastreams.dispatcher.Dispatcher;
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource;
import com.daml.telemetry.TelemetryContext;

import akka.NotUsed;
import akka.stream.javadsl.Source;
import io.vavr.API;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.compat.java8.FutureConverters;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.jdk.CollectionConverters;
import scala.math.Ordering;

/**
 *
 * @param <I>
 * @param <A>
 */
public final class Participant<I extends Identifier, A extends LedgerAddress> implements LedgerReader, LedgerWriter {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(Participant.class);
  private final TransactionLogReader<java.lang.Long, Raw.LogEntryId, Raw.Envelope> txLog;
  private final CommitPayloadBuilder<I> commitPayloadBuilder;
  private final LedgerSubmitter<I, A> submitter;
  private final String ledgerId;
  private final String participantId;
  private final Dispatcher<java.lang.Long> dispatcher;
  private final ExecutionContext context;

  /**
   * Convenience method for creating a builder.
   *
   * @param theParticipantId
   * @param theLedgerId
   * @param theContext
   * @param <I2>
   * @param <A2>
   * @return A partially configured participant builder.
   */
  public static <I2 extends Identifier, A2 extends LedgerAddress> ParticipantBuilder<I2, A2> builder(
      final String theParticipantId, final String theLedgerId, final ResourceContext theContext) {
    return new ParticipantBuilder<>(theParticipantId, theLedgerId, theContext);
  }

  /**
   * @param theTxLog
   * @param theCommitPayloadBuilder
   * @param theSubmitter
   * @param theLedgerId
   * @param theParticipantId
   * @param theDispatcher
   * @param theContext
   */
  public Participant(final TransactionLogReader<Long, Raw.LogEntryId, Raw.Envelope> theTxLog,
      final CommitPayloadBuilder<I> theCommitPayloadBuilder, final LedgerSubmitter<I, A> theSubmitter,
      final String theLedgerId, final String theParticipantId, final Dispatcher<Long> theDispatcher,
      final ExecutionContext theContext) {
    txLog = theTxLog;
    commitPayloadBuilder = theCommitPayloadBuilder;
    submitter = theSubmitter;
    ledgerId = theLedgerId;
    participantId = theParticipantId;
    dispatcher = theDispatcher;
    context = theContext;
  }

  @Override
  public HealthStatus currentHealth() {
    return HealthStatus.healthy();
  }

  @Override
  public akka.stream.scaladsl.Source<LedgerRecord, NotUsed> events(final Option<Offset> startExclusive) {
    LOG.info("Get from {}", () -> startExclusive);

    var start = OffsetBuilder.fromLong(0L, 0, 0);

    Ordering<Long> scalaLongOrdering = scala.math.Ordering
        .comparatorToOrdering(Comparator.comparingLong(scala.Long::unbox));

    /// Documentation says that s is exclusive start, but this does not seem to be the required
    /// behaviour
    var rangeSource = new RangeSource<>((s, e) -> Source
        .fromJavaStream(() -> API.unchecked(() -> txLog.from(s - 1, Optional.of(e - 1))).apply()
            .map(r -> Tuple2.apply(r._1, LedgerRecord.apply(OffsetBuilder.fromLong(r._1, 0, 0), r._2, r._3))))
        .mapMaterializedValue(m -> NotUsed.notUsed()).asScala(), scalaLongOrdering);

    var offset = OffsetBuilder.highestIndex(startExclusive.getOrElse(() -> start));
    return dispatcher.startingAt(offset, rangeSource, Option.empty()).asJava().map(x -> x._2).asScala();
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

    var payloads = commitPayloadBuilder.build(envelope, metadata, correlationId).stream().map(submitter::submitPayload)
        .map(f -> f.thenApply(x -> {
          if (x == SubmissionStatus.OVERLOADED) {
            return SubmissionResult.Overloaded$.MODULE$;
          } else if (x == SubmissionStatus.REJECTED) {
            return SubmissionResult.NotSupported$.MODULE$;
          }
          return SubmissionResult.Acknowledged$.MODULE$;
        })).map(FutureConverters::toScala).collect(Collectors.toList());

    return Future.foldLeft(CollectionConverters.CollectionHasAsScala(payloads).asScala().toSeq(),
        SubmissionResult.Acknowledged$.MODULE$,
        // Folding latch to any failed payload, unknown how this will affect retry behaviour without
        // atomicity
        (a, x) -> {
          if (a == SubmissionResult.Acknowledged$.MODULE$ && x == SubmissionResult.Acknowledged$.MODULE$) {
            return SubmissionResult.Acknowledged$.MODULE$;
          }

          return x;
        }, context);
  }
}

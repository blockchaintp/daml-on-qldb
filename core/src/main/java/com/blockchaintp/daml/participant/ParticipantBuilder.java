/*
 * Copyright © 2023 Paravela Limited
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
import java.util.function.UnaryOperator;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.address.LedgerAddress;
import com.blockchaintp.daml.stores.layers.Bijection;
import com.blockchaintp.daml.stores.layers.CoercingTxLog;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.TransactionLogReader;
import com.blockchaintp.exception.BuilderException;
import com.blockchaintp.utility.UuidConverter;
import com.daml.ledger.participant.state.kvutils.Raw;
import com.daml.platform.akkastreams.dispatcher.Dispatcher$;
import com.google.protobuf.ByteString;

import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import scala.concurrent.ExecutionContextExecutor;

/**
 *
 * @param <I>
 * @param <A>
 */
@SuppressWarnings("checkstyle:RegexpSingleline")
public final class ParticipantBuilder<I extends Identifier, A extends LedgerAddress> {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(ParticipantBuilder.class);
  private final String participantId;
  private final String ledgerId;
  private final ExecutionContextExecutor context;
  private TransactionLogReader<Long, Raw.LogEntryId, Raw.Envelope> txLog;
  private final CommitPayloadBuilder<I> commitPayloadBuilder;
  private InProcLedgerSubmitterBuilder<I, A> submitterBuilder;

  /**
   * Construct a participant builder for the given identifiers.
   *
   * @param theLedgerId
   * @param theParticipantId
   * @param theContext
   */
  public ParticipantBuilder(final String theLedgerId, final String theParticipantId,
      final ExecutionContextExecutor theContext) {
    participantId = theParticipantId;
    ledgerId = theLedgerId;
    context = theContext;
    commitPayloadBuilder = new CommitPayloadBuilder<>(participantId);
  }

  /**
   * Add a transaction log reader to the participant.
   *
   * @param reader
   * @return The configured builder.
   */
  public ParticipantBuilder<I, A> withTransactionLogReader(final TransactionLog<UUID, ByteString, Long> reader) {
    this.txLog = CoercingTxLog.from(Bijection.of(UuidConverter::logEntryToUuid, UuidConverter::uuidtoLogEntry),
        Bijection.of(Raw.Envelope::bytes, Raw.Envelope$.MODULE$::apply), Bijection.identity(), reader);

    return this;
  }

  /**
   * Add a ledger submitter to the participant.
   *
   * @param theSubmitterBuilder
   * @return The configured builder.
   */
  public ParticipantBuilder<I, A> withInProcLedgerSubmitterBuilder(
      final UnaryOperator<InProcLedgerSubmitterBuilder<I, A>> theSubmitterBuilder) {
    if (submitterBuilder == null) {
      submitterBuilder = new InProcLedgerSubmitterBuilder<>();
    }

    submitterBuilder = theSubmitterBuilder.apply(submitterBuilder);

    return this;
  }

  /**
   * Add a commit payload builder to this participant.
   *
   * @param configure
   * @return The configured builder.
   */
  public ParticipantBuilder<I, A> configureCommitPayloadBuilder(
      final UnaryOperator<CommitPayloadBuilder<I>> configure) {
    configure.apply(commitPayloadBuilder);

    return this;
  }

  /**
   * Build the participant.
   *
   * @return A configured participant.
   * @throws BuilderException
   */
  public Participant<I, A> build() throws BuilderException {
    if (txLog == null) {
      throw new BuilderException("Participant requires a transaction log");
    }
    if (submitterBuilder == null) {
      throw new BuilderException("Participant requires a configured submitter builder");
    }

    /// Log offset is the offset of the last written log, daml wants a head indicator - the end of the
    /// stream
    var logOffset = txLog.getLatestOffset();

    LOG.info("Ledger head at {}", () -> logOffset);
    /// Defer this construction
    var dispatcher = Dispatcher$.MODULE$.apply("daml-on-qldb", 0L, logOffset.map(x -> x + 1).orElse(0L),
        scala.math.Ordering.comparatorToOrdering(Long::compare));

    return new Participant<>(txLog, commitPayloadBuilder, submitterBuilder.withDispatcher(dispatcher).build(), ledgerId,
        participantId, dispatcher, context);
  }
}

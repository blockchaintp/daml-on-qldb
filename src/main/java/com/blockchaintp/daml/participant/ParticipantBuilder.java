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
import java.util.function.UnaryOperator;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.address.LedgerAddress;
import com.blockchaintp.daml.stores.layers.CoercingTxLog;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.TransactionLogReader;
import com.blockchaintp.exception.BuilderException;
import com.blockchaintp.utility.UuidConverter;
import com.daml.ledger.participant.state.kvutils.Raw;
import com.daml.ledger.participant.state.v1.Offset;
import com.daml.ledger.participant.state.v1.Offset$;
import com.daml.ledger.resources.ResourceContext;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;

/**
 *
 * @param <I>
 * @param <A>
 */
@SuppressWarnings("checkstyle:RegexpSingleline")
public final class ParticipantBuilder<I extends Identifier, A extends LedgerAddress> {
  private final String participantId;
  private final String ledgerId;
  private final ResourceContext context;
  private TransactionLogReader<Offset, Raw.LogEntryId, Raw.Envelope> txLog;
  private final CommitPayloadBuilder<I> commitPayloadBuilder;
  private InProcLedgerSubmitterBuilder<I, A> submitterBuilder;

  /**
   * Construct a participant builder for the given identifiers.
   *
   * @param theLedgerId
   * @param theParticipantId
   * @param theContext
   */
  public ParticipantBuilder(final String theLedgerId, final String theParticipantId, final ResourceContext theContext) {
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
    this.txLog = CoercingTxLog.readerFrom(
        (UUID k) -> Raw.LogEntryId$.MODULE$.apply(ByteString.copyFrom(UuidConverter.asBytes(k))),
        Raw.Envelope$.MODULE$::apply, (Long i) -> Offset$.MODULE$.fromByteArray(Longs.toByteArray(i)),
        (Raw.LogEntryId k) -> UuidConverter.asUuid(k.bytes().toByteArray()), Raw.Envelope::bytes,
        (Offset i) -> Longs.fromByteArray(i.toByteArray()), reader);

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

    return new Participant<>(txLog, commitPayloadBuilder,
        submitterBuilder.withParticipantId(participantId).withExecutionContext(context.executionContext()).build(),
        ledgerId, participantId, context.executionContext());
  }
}

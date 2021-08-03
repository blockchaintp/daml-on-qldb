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

import java.util.function.UnaryOperator;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.address.LedgerAddress;
import com.blockchaintp.daml.stores.service.TransactionLogReader;
import com.blockchaintp.exception.BuilderException;
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord;
import com.daml.ledger.participant.state.v1.Offset;
import com.daml.ledger.resources.ResourceContext;

/**
 *
 * @param <I>
 * @param <A>
 */
@SuppressWarnings("checkstyle:RegexpSingleline")
public final class ParticipantBuilder<I extends Identifier, A extends LedgerAddress> {
  private final String participantId;
  private final String ledgerId;
  private TransactionLogReader<Offset, I, LedgerRecord> txLog;
  private LedgerSubmitter<I, A> submitter;
  private final CommitPayloadBuilder commitPayloadBuilder;

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
    commitPayloadBuilder = new CommitPayloadBuilder(participantId);
  }

  /**
   * Add a transaction log reader to the participant.
   *
   * @param reader
   * @return The configured builder.
   */
  public ParticipantBuilder<I, A> withTransactionLogReader(final TransactionLogReader<Offset, I, LedgerRecord> reader) {
    this.txLog = reader;

    return this;
  }

  /**
   * Add a ledger submitter to the participant.
   *
   * @param theSubmitter
   * @return The configured builder.
   */
  public ParticipantBuilder<I, A> withLedgerSubmitter(final LedgerSubmitter<I, A> theSubmitter) {
    this.submitter = theSubmitter;

    return this;
  }

  /**
   * Add a commit payload builder to this participant.
   *
   * @param configure
   * @return The configured builder.
   */
  public ParticipantBuilder<I, A> configureCommitPayloadBuilder(final UnaryOperator<CommitPayloadBuilder> configure) {
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
    if (submitter == null) {
      throw new BuilderException("Participant requires a configured submitter");
    }

    return new Participant<I, A>(txLog, commitPayloadBuilder, submitter, ledgerId, participantId);
  }
}

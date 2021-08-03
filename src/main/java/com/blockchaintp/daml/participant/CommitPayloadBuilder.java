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

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.protobuf.DamlOperation;
import com.blockchaintp.daml.protobuf.DamlTransaction;
import com.blockchaintp.exception.BuilderException;
import com.daml.ledger.participant.state.kvutils.Raw;
import com.daml.ledger.participant.state.kvutils.api.CommitMetadata;
import com.google.protobuf.ByteString;

/**
 *
 * @param <A>
 */
public final class CommitPayloadBuilder<A extends Identifier> {

  /**
   * Behaviours for breaking down a large submission into multiple fragments to be re-assembled
   * elsewhere.
   */
  public interface FragmentationStrategy {
    /**
     *
     * @param theEnvelope
     * @param theLogEntryId
     * @param theCorrelationId
     * @return One or more daml operations to be committed.
     */
    List<DamlOperation> fragment(Raw.Envelope theEnvelope, ByteString theLogEntryId, String theCorrelationId);
  }

  /**
   *
   */
  public final class NoFragmentation implements FragmentationStrategy {
    private final String participantId;

    /**
     * Package the submission as a single transaction, do not fragment.
     *
     * @param theParticipantId
     */
    public NoFragmentation(final String theParticipantId) {
      participantId = theParticipantId;
    }

    @Override
    public List<DamlOperation> fragment(final Raw.Envelope theEnvelope, final ByteString theLogEntryId,
        final String theCorrelationId) {
      final var tx = DamlTransaction.newBuilder().setSubmission(theEnvelope.bytes()).setLogEntryId(theLogEntryId)
          .build();
      return Arrays.asList(DamlOperation.newBuilder().setCorrelationId(theCorrelationId)
          .setSubmittingParticipant(participantId).setTransaction(tx).build());
    }
  }

  private FragmentationStrategy fragmentationStrategy;
  private Function<CommitMetadata, Stream<A>> outputAddressReader;
  private Function<CommitMetadata, Stream<A>> inputAddressReader;
  private final String participantId;

  /**
   *
   * @param theParticipantId
   */
  public CommitPayloadBuilder(final String theParticipantId) {
    participantId = theParticipantId;
    withNoFragmentation();
  }

  /**
   *
   * @param theAddressReader
   * @return A configured builder.
   */
  public CommitPayloadBuilder withInputAddressReader(final Function<CommitMetadata, Stream<A>> theAddressReader) {
    this.inputAddressReader = theAddressReader;

    return this;
  }

  /**
   *
   * @param theAddressReader
   * @return A configured builder.
   */
  public CommitPayloadBuilder withOutputAddressReader(final Function<CommitMetadata, Stream<A>> theAddressReader) {
    this.outputAddressReader = theAddressReader;

    return this;
  }

  /**
   * Do not fragment commits before submission.
   *
   * @return A configured builder.
   */
  public CommitPayloadBuilder<A> withNoFragmentation() {
    this.fragmentationStrategy = new NoFragmentation(participantId);

    return this;
  }

  /**
   * Build a number of commit payloads depending on the configured fragmentation strategy.
   *
   * @param theEnvelope
   * @param metadata
   * @param correlationId
   * @return Payloads to commit.
   */
  public List<CommitPayload<A>> build(final Raw.Envelope theEnvelope, final CommitMetadata metadata,
      final String correlationId) {
    if (fragmentationStrategy == null) {
      throw new BuilderException("Commit payload builders need a fragmentation strategy");
    }
    if (fragmentationStrategy == null) {
      throw new BuilderException("Commit payload builders need a fragmentation strategy");
    }
    return fragmentationStrategy.fragment(theEnvelope, null, correlationId).stream()
        .map(op -> new CommitPayload<A>(op, metadata, inputAddressReader, outputAddressReader))
        .collect(Collectors.toList());
  }

}

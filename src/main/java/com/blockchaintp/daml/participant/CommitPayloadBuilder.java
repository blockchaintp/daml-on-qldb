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
import com.blockchaintp.exception.BuilderException;
import com.daml.ledger.participant.state.kvutils.Raw;
import com.daml.ledger.participant.state.kvutils.api.CommitMetadata;

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
     * @param theCorrelationId
     * @return One or more daml operations to be committed.
     */
    List<Raw.Envelope> fragment(Raw.Envelope theEnvelope, String theCorrelationId);
  }

  /**
   *
   */
  public static final class NoFragmentation implements FragmentationStrategy {
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
    public List<Raw.Envelope> fragment(final Raw.Envelope theEnvelope, final String theCorrelationId) {
      return Arrays.asList(theEnvelope);
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
  public CommitPayloadBuilder<A> withInputAddressReader(final Function<CommitMetadata, Stream<A>> theAddressReader) {
    this.inputAddressReader = theAddressReader;

    return this;
  }

  /**
   *
   * @param theAddressReader
   * @return A configured builder.
   */
  public CommitPayloadBuilder<A> withOutputAddressReader(final Function<CommitMetadata, Stream<A>> theAddressReader) {
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
      final String correlationId) throws BuilderException {
    if (inputAddressReader == null) {
      throw new BuilderException("No configured input address reader");
    }
    if (outputAddressReader == null) {
      throw new BuilderException("No configured output address reader");
    }
    return fragmentationStrategy.fragment(theEnvelope, correlationId).stream().map(
        op -> new CommitPayload<A>(op, correlationId, participantId, metadata, inputAddressReader, outputAddressReader))
        .collect(Collectors.toList());
  }

}

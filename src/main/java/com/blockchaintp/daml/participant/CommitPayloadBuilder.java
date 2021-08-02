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
import java.util.function.BiFunction;
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
  private BiFunction<Raw.Envelope, String, List<DamlOperation>> fragmentationStrategy;
  private Function<CommitMetadata, Stream<A>> outputAddressReader;
  private Function<CommitMetadata, Stream<A>> inputAddressReader;

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
   * A non fragmenting strategy.
   *
   * @param logEntryId
   * @param correlationId
   * @param participantId
   * @param data
   * @return A single operation.
   */
  public static List<DamlOperation> noFragmentation(final ByteString logEntryId, final String correlationId,
      final String participantId, final Raw.Envelope data) {
    final var tx = DamlTransaction.newBuilder().setSubmission(data.bytes()).setLogEntryId(logEntryId).build();
    return Arrays.asList(DamlOperation.newBuilder().setCorrelationId(correlationId)
        .setSubmittingParticipant(participantId).setTransaction(tx).build());
  }

  /**
   *
   * @param theStrategy
   * @return The configured builder.
   */
  public CommitPayloadBuilder<A> withFragmentationStrategy(
      final BiFunction<Raw.Envelope, String, List<DamlOperation>> theStrategy) {
    this.fragmentationStrategy = theStrategy;

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
    return fragmentationStrategy.apply(theEnvelope, correlationId).stream()
        .map(op -> new CommitPayload<A>(op, metadata, inputAddressReader, outputAddressReader))
        .collect(Collectors.toList());
  }

}

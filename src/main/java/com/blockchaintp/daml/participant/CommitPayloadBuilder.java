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
import com.daml.ledger.participant.state.kvutils.Raw;
import com.daml.ledger.participant.state.kvutils.api.CommitMetadata;
import com.google.protobuf.ByteString;

/**
 *
 * @param <A>
 */
public final class CommitPayloadBuilder<A extends Identifier> {
  private BiFunction<Raw.Envelope, String, List<DamlOperation>> fragmentationStrategy;
  private Function<CommitMetadata, Stream<A>> inputAddressReader;
  private Function<CommitMetadata, Stream<A>> outputAddressReader;

  /**
   *
   */
  public CommitPayloadBuilder() {

  }

  /**
   * A non fragmenting strategy.
   * @param logEntryId
   * @param correlationId
   * @param participantId
   * @param data
   * @return A single operation.
   */
  public static List<DamlOperation> noFragmentation(final ByteString logEntryId, final String correlationId, final String participantId, final Raw.Envelope data) {
    final var tx = DamlTransaction.newBuilder().setSubmission(data.bytes())
      .setLogEntryId(logEntryId)
      .build();
    return Arrays.asList(DamlOperation.newBuilder().setCorrelationId(correlationId)
      .setSubmittingParticipant(participantId).setTransaction(tx).build());
  }

  /**
   *
   * @param theStrategy
   * @return The configured builder.
   */
  public CommitPayloadBuilder<A> withFragmentationStrategy(final BiFunction<Raw.Envelope, String, List<DamlOperation>> theStrategy) {
    this.fragmentationStrategy = theStrategy;

    return this;
  }

  /**
   * Build a number of commit payloads depending on the configured fragmentation strategy.
   * @param theEnvelope
   * @param metadata
   * @param correlationId
   * @return
   */
  public List<CommitPayload<A>> build(final Raw.Envelope theEnvelope, final CommitMetadata metadata, final String correlationId) {
    return fragmentationStrategy.apply(theEnvelope, correlationId)
      .stream().map(op -> new CommitPayload<A>(
        op,
        metadata,
        inputAddressReader,
        outputAddressReader
      )
    ).collect(Collectors.toList());
  }


}

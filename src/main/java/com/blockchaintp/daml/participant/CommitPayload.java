package com.blockchaintp.daml.participant;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.protobuf.DamlOperation;
import com.blockchaintp.daml.protobuf.DamlTransaction;
import com.daml.ledger.participant.state.kvutils.api.CommitMetadata;

/**
 * Represents the assembled data that may be required to submit a transaction.
 * @param <A> the type of the identifier (e.g. {@link LedgerAddress} or {@link Identifier})
 */
public class CommitPayload<A extends Identifier> {
  private final CommitMetadata metadata;
  private DamlOperation operation;
  private Set<A> reads;
  private Set<A> writes;

  /**
   * Create a builder for payloads.
   * @param <A1>
   * @return A builder
   */
  public static <A1 extends Identifier> CommitPayloadBuilder<A1> builder() {
    return new CommitPayloadBuilder<A1>();
  }

  /**
   *
   * @param theOperation
   * @param theMetadata
   * @param readAddressExtractor
   * @param writeAddressExtractor
   */
  public CommitPayload(final DamlOperation theOperation, final CommitMetadata theMetadata, final Function<CommitMetadata, Stream<A>> readAddressExtractor, final Function<CommitMetadata, Stream<A>> writeAddressExtractor)  {
    this.operation = theOperation;
    this.metadata = theMetadata;
    this.reads = readAddressExtractor.apply(theMetadata).collect(Collectors.toSet());
    this.writes = writeAddressExtractor.apply(theMetadata).collect(Collectors.toSet());
  }

  /**
   * @return the transaction
   */
  public DamlOperation getOperation() {
    return operation;
  }
}

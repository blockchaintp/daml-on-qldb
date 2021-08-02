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

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.protobuf.DamlOperation;
import com.daml.ledger.participant.state.kvutils.api.CommitMetadata;

/**
 * Represents the assembled data that may be required to submit a transaction.
 *
 * @param <A>
 *          the type of the identifier (e.g. {@link LedgerAddress} or {@link Identifier})
 */
public class CommitPayload<A extends Identifier> {
  private final CommitMetadata metadata;
  private DamlOperation operation;
  private Set<A> reads;
  private Set<A> writes;

  /**
   * Create a builder for payloads.
   *
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
  public CommitPayload(final DamlOperation theOperation, final CommitMetadata theMetadata,
      final Function<CommitMetadata, Stream<A>> readAddressExtractor,
      final Function<CommitMetadata, Stream<A>> writeAddressExtractor) {
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

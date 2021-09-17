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
import com.daml.ledger.participant.state.kvutils.Raw;
import com.daml.ledger.participant.state.kvutils.api.CommitMetadata;

/**
 * Represents the assembled data that may be required to submit a transaction.
 *
 * @param <A>
 *          the type of the identifier (e.g. {@link LedgerAddress} or {@link Identifier})
 */
public final class CommitPayload<A extends Identifier> {
  private final Raw.Envelope submission;
  private final String correlationId;
  private final String submittingParticipantId;
  private final Set<A> reads;
  private final Set<A> writes;

  /**
   * @param theSubmission
   * @param theCorrelationId
   * @param theSubmittingParticipantId
   * @param theMetadata
   * @param readAddressExtractor
   * @param writeAddressExtractor
   */
  public CommitPayload(final Raw.Envelope theSubmission, final String theCorrelationId,
      final String theSubmittingParticipantId, final CommitMetadata theMetadata,
      final Function<CommitMetadata, Stream<A>> readAddressExtractor,
      final Function<CommitMetadata, Stream<A>> writeAddressExtractor) {
    this.submission = theSubmission;
    correlationId = theCorrelationId;
    submittingParticipantId = theSubmittingParticipantId;
    this.reads = readAddressExtractor.apply(theMetadata).collect(Collectors.toSet());
    this.writes = writeAddressExtractor.apply(theMetadata).collect(Collectors.toSet());
  }

  /**
   * @return the sumission.
   */
  public Raw.Envelope getSubmission() {
    return submission;
  }

  /**
   *
   * @return The set of read addresses.
   */
  public Set<A> getReads() {
    return reads;
  }

  /**
   *
   * @return The set of write addresses.
   */
  public Set<A> getWrites() {
    return writes;
  }

  /**
   *
   * @return The correlation id.
   */
  public String getCorrelationId() {
    return correlationId;
  }

  /**
   *
   * @return The submitting participant id.
   */
  public String getSubmittingParticipantId() {
    return submittingParticipantId;
  }
}

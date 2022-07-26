/*
 * Copyright 2022 Blockchain Technology Partners
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

import java.util.Optional;

/**
 * The type Submission result.
 */
public class SubmissionResult {
  private final SubmissionStatus status;

  /**
   * Gets status.
   *
   * @return the status
   */
  public SubmissionStatus getStatus() {
    return status;
  }

  /**
   * Gets error.
   *
   * @return the synchronous error
   */
  public Optional<com.daml.ledger.participant.state.v2.SubmissionResult.SynchronousError> getError() {
    return error;
  }

  private final Optional<com.daml.ledger.participant.state.v2.SubmissionResult.SynchronousError> error;

  /**
   * Instantiates a new Submission result.
   *
   * @param theStatus
   *          the the status
   * @param theError
   *          the the error
   */
  public SubmissionResult(final SubmissionStatus theStatus,
      final Optional<com.daml.ledger.participant.state.v2.SubmissionResult.SynchronousError> theError) {
    status = theStatus;
    error = theError;
  }

  /**
   * Submitted submission result.
   *
   * @return the submission result
   */
  public static SubmissionResult submitted() {
    return new SubmissionResult(SubmissionStatus.SUBMITTED, Optional.empty());
  }

  /**
   * The participant cannot currently process this submission.
   * 
   * @return the submission result
   */
  public static SubmissionResult overloaded() {
    return new SubmissionResult(SubmissionStatus.OVERLOADED, Optional.empty());
  }

  /**
   * The participant has not yet submitted a proposal.
   *
   * @return the submission result
   */
  public static SubmissionResult enqueued() {
    return new SubmissionResult(SubmissionStatus.ENQUEUED, Optional.empty());
  }

  /**
   * The participant has submitted a proposal, but it has not yet been fully accepted. This is
   * normally only applicable to multipart submissions
   *
   * @return the submission result
   */
  public static SubmissionResult partiallySubmitted() {
    return new SubmissionResult(SubmissionStatus.PARTIALLY_SUBMITTED, Optional.empty());
  }

  /**
   * The participant has submitted a proposal, and it has been rejected. This is a terminal state and
   * is normally quite immediate.
   *
   * @param error
   *          the error
   * @return the submission result
   */
  public static SubmissionResult rejected(
      final com.daml.ledger.participant.state.v2.SubmissionResult.SynchronousError error) {
    return new SubmissionResult(SubmissionStatus.REJECTED, Optional.of(error));
  }
}

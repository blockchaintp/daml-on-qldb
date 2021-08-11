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

/**
 * The submission status of a particular submission.
 */
public enum SubmissionStatus {
  /**
   * The participant has not yet submitted a proposal.
   */
  ENQUEUED,
  /**
   * The participant has submitted a proposal, but it has not yet been fully accepted. This is
   * normally only applicable to multipart submissions
   */
  PARTIALLY_SUBMITTED,
  /**
   * The participant has submitted a proposal, and it has been rejected. This is a terminal state and
   * is normally quite immediate.
   */
  REJECTED,
  /**
   * The participant has submitted a proposal, and it has been accepted. The submitter may safely
   * forget about the submission process.
   */
  SUBMITTED
}

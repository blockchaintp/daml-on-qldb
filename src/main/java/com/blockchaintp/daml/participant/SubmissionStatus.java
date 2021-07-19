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
   * The participant has submitted a proposal, but it has not yet been fully
   * accepted. This is normally only applicable to multipart submissions
   */
  PARTIALLY_SUBMITTED,
  /**
   * The participant has submitted a proposal, and it has been rejected. This is a
   * terminal state and is normally quite immediate.
   */
  REJECTED,
  /**
   * The participant has submitted a proposal, and it has been accepted. The
   * submitter may safely forget about the submission process.
   */
  SUBMITTED
}

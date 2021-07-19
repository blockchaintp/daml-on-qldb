package com.blockchaintp.daml.participant;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.address.LedgerAddress;

/**
 * A LedgerSubmitter is responsible for taking a commit payload based on one identifier
 * and submitting it to the ledger with its own addressing scheme.
 * @param <A> the type of the identifier used in submissions.
 * @param <B> the type of the LedgerAddress used in the ledger.
 */
public interface LedgerSubmitter<A extends Identifier, B extends LedgerAddress> {
  /**
   * Submit the payload to the ledger.
   * @param cp the payload to submit.
   * @return a reference to the submission.
   */
  SubmissionReference submitPayload(CommitPayload<A> cp);

  /**
   * Check the status of a submission.
   * @param ref the reference to check.
   * @return a status object.
   */
  SubmissionStatus checkSubmission(SubmissionReference ref);

  /**
   * For convenience we can translate one the payload to the expected output payload.
   * @param cp the input payload.
   * @return the output payload.
   */
  CommitPayload<B> translatePayload(CommitPayload<A> cp);
}

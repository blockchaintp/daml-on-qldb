/*
 * Copyright 2021-2022 Blockchain Technology Partners
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

import java.util.concurrent.CompletableFuture;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.address.LedgerAddress;

/**
 * A LedgerSubmitter is responsible for taking a commit payload based on one identifier and
 * submitting it to the ledger with its own addressing scheme.
 *
 * @param <A>
 *          the type of the identifier used in submissions.
 * @param <B>
 *          the type of the LedgerAddress used in the ledger.
 */
public interface LedgerSubmitter<A extends Identifier, B extends LedgerAddress> {
  /**
   * Submit the payload to the ledger.
   *
   * @param cp
   *          the payload to submit.
   * @return a reference to the submission.
   */
  CompletableFuture<SubmissionResult> submitPayload(CommitPayload<A> cp);

  /**
   * For convenience we can translate one the payload to the expected output payload.
   *
   * @param cp
   *          the input payload.
   * @return the output payload.
   */
  CommitPayload<B> translatePayload(CommitPayload<A> cp);
}

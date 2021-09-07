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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.address.LedgerAddress;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.decorators.Decorators;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;

/**
 * Resilience for an underlying ledgersubmitter.
 *
 * @param <A>
 * @param <B>
 */
public final class LedgerSubmitterBulkhead<A extends Identifier, B extends LedgerAddress>
    implements LedgerSubmitter<A, B> {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(InProcLedgerSubmitter.class);
  private static final int MAX_CONCURRENT = 50;
  private final LedgerSubmitter<A, B> inner;
  private final CircuitBreaker circuitBreaker;
  private final Bulkhead bulkhead;

  /**
   *
   * @param theInner
   */
  public LedgerSubmitterBulkhead(final LedgerSubmitter<A, B> theInner) {
    inner = theInner;
    // Create a CircuitBreaker with default configuration
    circuitBreaker = CircuitBreaker.of("ledger-submitter", CircuitBreakerConfig.ofDefaults().custom().build());

    // Create a Bulkhead with default configuration
    bulkhead = Bulkhead.of("ledger-submitter", BulkheadConfig.custom().maxConcurrentCalls(MAX_CONCURRENT).build());
  }

  @Override
  public CompletableFuture<SubmissionStatus> submitPayload(final CommitPayload<A> cp) {
    return Decorators.ofCompletionStage(() -> inner.submitPayload(cp)).withCircuitBreaker(circuitBreaker)
        .withBulkhead(bulkhead)
        .withFallback(
            Arrays.asList(TimeoutException.class, CallNotPermittedException.class, BulkheadFullException.class),
            throwable -> SubmissionStatus.OVERLOADED)
        .get().toCompletableFuture();
  }
}

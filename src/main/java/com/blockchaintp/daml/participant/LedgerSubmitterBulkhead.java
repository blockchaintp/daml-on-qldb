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

import java.time.Duration;
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
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(LedgerSubmitterBulkhead.class);
  private static final int MINIMUM_NUMBER_OF_CALLS = 100;
  private final LedgerSubmitter<A, B> inner;
  private final CircuitBreaker circuitBreaker;
  private final Bulkhead bulkhead;

  /**
   * @param theInner
   * @param maxConcurrent
   * @param slowCallDuration
   */
  public LedgerSubmitterBulkhead(final LedgerSubmitter<A, B> theInner, final int maxConcurrent,
      final long slowCallDuration) {
    inner = theInner;
    // Create a CircuitBreaker with default configuration
    circuitBreaker = CircuitBreaker.of("ledger-submitter",
        CircuitBreakerConfig.custom().enableAutomaticTransitionFromOpenToHalfOpen()
            .minimumNumberOfCalls(MINIMUM_NUMBER_OF_CALLS)
            .slowCallDurationThreshold(Duration.ofMillis(slowCallDuration)).build());

    circuitBreaker.getEventPublisher()
        .onCallNotPermitted(r -> LOG.info("Ledger submitted circuit breaker deny call {}", r))
        .onStateTransition(r -> LOG.info("Ledger submitted circuit breaker state", r.toString()));

    // Create a Bulkhead with default configuration
    bulkhead = Bulkhead.of("ledger-submitter", BulkheadConfig.custom().maxConcurrentCalls(maxConcurrent).build());

    bulkhead.getEventPublisher().onCallRejected(r -> LOG.info("Bulkhead rejected call {}", r.toString()));
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

  @Override
  public CommitPayload<B> translatePayload(final CommitPayload<A> cp) {
    throw new UnsupportedOperationException();
  }
}

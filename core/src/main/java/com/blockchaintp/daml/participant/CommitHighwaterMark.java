/*
 * Copyright © 2023 Paravela Limited
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;

/**
 * Control commits so we can report them in sequence order.
 */
public final class CommitHighwaterMark {
  private static final int POLL_INTERVAL = 2;
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(CommitHighwaterMark.class);
  private final ConcurrentSkipListSet<Long> open = new ConcurrentSkipListSet<>();

  /**
   *
   * @param t
   */
  public void begin(final Long t) {
    synchronized (this) {
      open.add(t);
    }
  }

  /**
   * Determine the highest possible transaction to signal as complete and update our state - all
   * preceding transactions must also be complete.
   *
   * @param theCommit
   * @return A value if this is the highest commit, else empty.
   */
  public CompletableFuture<Long> highestCommitted(final Long theCommit) {

    synchronized (this) {
      open.add(theCommit);
    }

    var completionFuture = new CompletableFuture<Long>();
    var checkFuture = executor.scheduleAtFixedRate(() -> {
      synchronized (this) {
        LOG.info("Determine if {} is highest transaction open, before we allow it to complete {}", () -> theCommit,
            () -> open.stream().map(Object::toString).collect(Collectors.joining(",")));

        var highest = Optional.<Long>empty();

        if (open.lower(theCommit) == null) {
          highest = Optional.of(theCommit);
        }

        if (highest.isPresent()) {
          var toRemove = open.tailSet(highest.get(), true);
          LOG.debug("No transactions lower than {}, completing", highest.get());
          open.removeAll(toRemove);
          completionFuture.complete(highest.get());
        }
      }

    }, 0, POLL_INTERVAL, TimeUnit.MILLISECONDS);
    completionFuture.whenComplete((result, thrown) -> checkFuture.cancel(true));

    return completionFuture;
  }
}

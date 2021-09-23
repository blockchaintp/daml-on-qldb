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

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;

/**
 * Control commits so we can report them in sequence order.
 */
public final class CommitHighwaterMark {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(CommitHighwaterMark.class);
  private final ConcurrentSkipListSet<Long> open = new ConcurrentSkipListSet<>();
  private final ConcurrentSkipListSet<Long> complete = new ConcurrentSkipListSet<>();

  /**
   *
   * @param t
   */
  public void begin(final Long t) {
    open.add(t);
  }

  /**
   *
   * @param t
   */
  public void complete(final Long t) {
    complete.add(t);
  }

  /**
   *
   * @param t
   * @return Whether the transaction at t is still running.
   */
  public boolean running(final Long t) {
    return complete.contains(t);
  }

  /**
   * Determine the highest possible transaction to signal as complete and update our state - all
   * preceding transactions must also be complete.
   *
   * @param theCommit
   * @return A value if this is the highest commit, else empty.
   */
  public Optional<Long> highestCommitted(final Long theCommit) {
    synchronized (this) {
      complete.add(theCommit);
      open.remove(theCommit);
      LOG.trace("Determine if {} is highest transaction open {} complete {}", () -> theCommit,
          () -> open.stream().map(Object::toString).collect(Collectors.joining(",")),
          () -> complete.stream().map(Object::toString).collect(Collectors.joining(",")));

      var highest = Optional.<Long>empty();
      var candidates = complete.stream().sorted(Collections.reverseOrder()).collect(Collectors.toList());

      for (var x : candidates) {
        if (open.lower(x) == null) {
          highest = Optional.of(x);
        }
      }

      if (highest.isPresent()) {
        LOG.debug("Valid high water mark found at {}", highest.get());
        complete.removeAll(complete.headSet(highest.get(), true));
      }

      return highest;
    }
  }
}

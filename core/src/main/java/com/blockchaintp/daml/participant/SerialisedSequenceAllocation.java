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
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.TransactionLogWriter;
import com.daml.ledger.participant.state.kvutils.Raw;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;

/**
 * Endure store.begin completes in call order.
 */
public final class SerialisedSequenceAllocation {
  private static final int POLL_INTERVAL = 2;
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(SerialisedSequenceAllocation.class);
  private final BlockingDeque<Raw.LogEntryId> beginRx = new LinkedBlockingDeque<>();
  private final ConcurrentLinkedDeque<Tuple2<Raw.LogEntryId, Long>> beginTx = new ConcurrentLinkedDeque<>();
  private final TransactionLogWriter<Raw.LogEntryId, Raw.Envelope, Long> writer;
  private final CommitHighwaterMark highwaterMark;
  private final Executor workExecutor = Executors.newSingleThreadExecutor();
  private final ScheduledExecutorService pollExecutor = Executors.newSingleThreadScheduledExecutor();

  /**
   *
   * @param theWriter
   */
  public SerialisedSequenceAllocation(final TransactionLogWriter<Raw.LogEntryId, Raw.Envelope, Long> theWriter) {
    writer = theWriter;
    highwaterMark = new CommitHighwaterMark();
    workExecutor.execute(this::beginRunner);
  }

  /**
   * Block on our rx queue and do work sequentially.
   */
  private void beginRunner() {
    var running = true;
    while (running) {
      try {
        var next = beginRx.take();
        LOG.trace("Pre-poll begin rx {}", next);
        if (next != null) {
          executeBegin(next);
        }
      } catch (InterruptedException e) {
        LOG.info("Interrupted {}", e);
        running = false;
        Thread.currentThread().interrupt();
      }
    }
  }

  private void executeBegin(final Raw.LogEntryId next) {
    LOG.trace("Poll begin rx {}", next);
    var started = false;
    Tuple2<Raw.LogEntryId, Long> r = null;
    while (!started) {
      try {
        r = writer.begin(Optional.of(next));
        highwaterMark.begin(r._2);
        LOG.debug("Transaction seq {} {} begun, enqueuing", r._2, r._1);
        started = true;
      } catch (StoreWriteException e) {
        LOG.error("Failed to start transaction {}, retry", e);
      }
    }
    beginTx.add(Tuple.of(next, r._2));
  }

  /**
   *
   * @param commit
   * @return A future which only completes when all lower commits have completed.
   */
  public CompletableFuture<Long> serialisedCommit(final Long commit) {
    return highwaterMark.highestCommitted(commit);
  }

  /**
   *
   * @param key
   * @return A future that completes once the transaction has begun
   */
  public CompletableFuture<Long> serialisedBegin(final Raw.LogEntryId key) {
    LOG.info("Queue {} for begin", key);
    beginRx.add(key);

    var completionFuture = new CompletableFuture<Long>();
    var checkFuture = pollExecutor.scheduleAtFixedRate(() -> {
      synchronized (this) {
        var head = beginTx.peek();
        if ((head != null && head._1.bytes().equals(key.bytes()))) {
          completionFuture.complete(head._2);
          beginTx.pop();
        }
      }

    }, 0, POLL_INTERVAL, TimeUnit.MILLISECONDS);
    completionFuture.whenComplete((result, thrown) -> {
      LOG.debug("Cancel begin poll for {}", key);
      checkFuture.cancel(true);
    });

    return completionFuture;
  }
}

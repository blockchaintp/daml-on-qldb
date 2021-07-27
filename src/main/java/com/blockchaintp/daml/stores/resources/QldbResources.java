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
package com.blockchaintp.daml.stores.resources;

import java.util.concurrent.atomic.AtomicReference;

import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import software.amazon.awssdk.services.qldb.QldbClient;
import software.amazon.awssdk.services.qldb.model.CreateLedgerRequest;
import software.amazon.awssdk.services.qldb.model.DeleteLedgerRequest;
import software.amazon.awssdk.services.qldb.model.DescribeLedgerRequest;
import software.amazon.awssdk.services.qldb.model.LedgerState;
import software.amazon.awssdk.services.qldb.model.PermissionsMode;
import software.amazon.awssdk.services.qldb.model.ResourceNotFoundException;

/**
 * Deals with QLDB resources.
 */
public class QldbResources implements RequiresAWSResources {
  private static final int DEFAULT_WAIT_TIME_MS = 1000;
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(QldbResources.class);
  private final QldbClient infrastructureClient;
  private final String ledger;

  /**
   * Constructor.
   *
   * @param qldbClient
   *          the qldb client
   * @param ledgerName
   *          the ledger name
   */
  public QldbResources(final QldbClient qldbClient, final String ledgerName) {
    this.infrastructureClient = qldbClient;
    this.ledger = ledgerName;
  }

  private boolean ledgerState(final LedgerState state) {
    final AtomicReference<LedgerState> current = new AtomicReference<>(null);
    try {
      current.set(infrastructureClient.describeLedger(DescribeLedgerRequest.builder().name(ledger).build()).state());

      LOG.debug("Check ledger state, currently {}", current::get);

      return current.get().equals(state);
    } catch (Throwable e) {
      // TODO this one is bad, we should not catch all exceptions
      return current.get() == null && state.equals(LedgerState.DELETED);
    }
  }

  @Override
  public final void ensureResources() {
    LOG.debug("Check ledger state");
    if (ledgerState(LedgerState.ACTIVE)) {
      LOG.debug("Ledger {} exists, skip create", () -> ledger);
    } else {
      infrastructureClient.createLedger(CreateLedgerRequest.builder().name(ledger)
          .permissionsMode(PermissionsMode.STANDARD).deletionProtection(false).build());

      while (!ledgerState(LedgerState.ACTIVE)) {
        try {
          Thread.sleep(DEFAULT_WAIT_TIME_MS);
        } catch (InterruptedException e) {
          LOG.info("Interrupted while waiting for ledger {} to become active", () -> ledger);
          Thread.currentThread().interrupt();
        }
      }
    }

  }

  @Override
  public final void destroyResources() {
    if (ledgerState(LedgerState.DELETED)) {
      LOG.debug("Ledger {} does not exist, skip delete", () -> ledger);
      return;
    }
    LOG.info("Delete ledger {}", () -> ledger);

    try {
      infrastructureClient.deleteLedger(DeleteLedgerRequest.builder().name(ledger).build());
    } catch (ResourceNotFoundException e) {
      LOG.debug("Ledger does not exist");
      return;
    }

    while (!ledgerState(LedgerState.DELETED)) {
      try {
        Thread.sleep(DEFAULT_WAIT_TIME_MS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for ledger {} to be deleted", () -> ledger);
        Thread.currentThread().interrupt();
      }
    }
  }

}

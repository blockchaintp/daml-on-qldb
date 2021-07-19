package com.blockchaintp.daml.stores.resources;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import software.amazon.awssdk.services.qldb.QldbClient;
import software.amazon.awssdk.services.qldb.model.CreateLedgerRequest;
import software.amazon.awssdk.services.qldb.model.DeleteLedgerRequest;
import software.amazon.awssdk.services.qldb.model.DescribeLedgerRequest;
import software.amazon.awssdk.services.qldb.model.LedgerState;
import software.amazon.awssdk.services.qldb.model.PermissionsMode;
import software.amazon.awssdk.services.qldb.model.ResourceNotFoundException;
import software.amazon.qldb.QldbDriver;

/**
 * Deals with QLDB resources.
 */
public class QldbResources implements RequiresAWSResources {
  private static final int DEFAULT_WAIT_TIME_MS = 1000;
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(QldbResources.class);
  private final QldbClient infrastructureClient;
  private final QldbDriver driver;
  private final String ledger;
  private final String table;

  /**
   * Constructor.
   * @param qldbClient the qldb client
   * @param qldbDriver the qldb driver
   * @param ledgerName the ledger name
   * @param tableName the table name
   */
  public QldbResources(final QldbClient qldbClient, final QldbDriver qldbDriver, final String ledgerName,
      final String tableName) {
    this.infrastructureClient = qldbClient;
    this.driver = qldbDriver;
    this.ledger = ledgerName;
    this.table = tableName;
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

  private boolean tableExists() {
    return StreamSupport.stream(driver.getTableNames().spliterator(), false).anyMatch(s -> s.equals(table));
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

    if (tableExists()) {
      LOG.debug("Table {} exists, skip create", () -> table);
      return;
    }

    LOG.info("Creating table {}", () -> table);

    driver.execute(tx -> {
      tx.execute(String.format("create table %s", table));
      tx.execute(String.format("create index on %s(id)", table));
    });
  }

  @Override
  public final void destroyResources() {
    if (ledgerState(LedgerState.DELETED)) {
      LOG.debug("Ledger {} does not exist, skip delete", () -> table);
      return;
    }
    LOG.info("Delete ledger {}", () -> ledger);

    try {
      infrastructureClient
        .deleteLedger(DeleteLedgerRequest
          .builder()
          .name(ledger)
          .build());
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

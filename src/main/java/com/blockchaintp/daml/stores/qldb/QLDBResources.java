package com.blockchaintp.daml.stores.qldb;

import com.blockchaintp.daml.stores.RequiresAWSResources;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.qldb.QldbClient;
import software.amazon.awssdk.services.qldb.model.*;
import software.amazon.qldb.QldbDriver;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

public class QLDBResources implements RequiresAWSResources {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(QLDBResources.class);
  private QldbClient infrastructureClient;
  private QldbDriver driver;
  private String ledger;
  private String table;

  public QLDBResources(QldbClient infrastructureClient, QldbDriver driver, String ledger, String table) {
    this.infrastructureClient = infrastructureClient;
    this.driver = driver;
    this.ledger = ledger;
    this.table = table;
  }

  private boolean ledgerState(LedgerState state) {
    final AtomicReference<LedgerState> current = new AtomicReference<>(null);
    try {
      current.set(infrastructureClient.describeLedger(
        DescribeLedgerRequest
          .builder()
          .name(ledger)
          .build()
      ).state());

      LOG.debug("Check ledger state, currently {}", ()-> current.get());

      return current.get().equals(state);
    }
    catch (Throwable e) {
      if (current.get() == null && state.equals(LedgerState.DELETED)) {
        return true;
      }

      return false;
    }
  }

  private boolean tableExists() {
    return StreamSupport.stream(driver.getTableNames().spliterator(), false)
      .anyMatch(s -> s.equals(table));
  }

  @Override
  public void ensureResources() {
    LOG.debug("Check ledger state");
    if (ledgerState(LedgerState.ACTIVE)) {
      LOG.debug("Ledger {} exists, skip create", () -> ledger);
    } else {
      infrastructureClient
        .createLedger(CreateLedgerRequest
          .builder()
          .name(ledger)
          .permissionsMode(PermissionsMode.STANDARD)
          .deletionProtection(false)
          .build());

      while(!ledgerState(LedgerState.ACTIVE)) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
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
  public void destroyResources() {
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

    while(!ledgerState(LedgerState.DELETED)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

}

package com.blockchaintp.daml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazon.ion.IonValue;
import com.amazonaws.services.qldb.AmazonQLDB;
import com.amazonaws.services.qldb.AmazonQLDBClientBuilder;
import com.amazonaws.services.qldb.model.CreateLedgerRequest;
import com.amazonaws.services.qldb.model.CreateLedgerResult;
import com.amazonaws.services.qldb.model.DeleteLedgerRequest;
import com.amazonaws.services.qldb.model.DescribeLedgerRequest;
import com.amazonaws.services.qldb.model.DescribeLedgerResult;
import com.amazonaws.services.qldb.model.LedgerState;
import com.amazonaws.services.qldb.model.PermissionsMode;
import com.amazonaws.services.qldb.model.ResourceNotFoundException;
import com.amazonaws.services.qldbsession.model.BadRequestException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.qldb.PooledQldbDriver;
import software.amazon.qldb.QldbSession;
import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;

public class QLDBServiceClient {

  private static final Logger LOG = LoggerFactory.getLogger(QLDBServiceClient.class);
  private final AmazonQLDB client;

  public QLDBServiceClient() {
    this.client = AmazonQLDBClientBuilder.standard().build();
  }

  public CreateLedgerResult createLedger(final String ledgerName) {
    LOG.info("Creating ledger with name: {}", ledgerName);
    CreateLedgerRequest request = new CreateLedgerRequest().withName(ledgerName)
        .withPermissionsMode(PermissionsMode.ALLOW_ALL).withDeletionProtection(false);
    CreateLedgerResult result = this.client.createLedger(request);
    LOG.info("Create ledger with name: {} result: {}", ledgerName, result.getState());
    return result;
  }

  public DescribeLedgerResult describeLedger(final String ledgerName) {
    LOG.info("Describing ledger with name: {}", ledgerName);
    DescribeLedgerRequest request = new DescribeLedgerRequest().withName(ledgerName);
    DescribeLedgerResult result = this.client.describeLedger(request);
    return result;
  }

  public boolean checkLedgerActive(final String ledgerName) {
    try {
      return describeLedger(ledgerName).getState().equals(LedgerState.ACTIVE.name());
    } catch (ResourceNotFoundException e) {
      LOG.info("Ledger with name {} does not exist", ledgerName);
      return false;
    }
  }

  public boolean ledgerExists(final String ledgerName) {
    try {
      describeLedger(ledgerName).getState().equals(LedgerState.ACTIVE.name());
      return true;
    } catch (ResourceNotFoundException e) {
      LOG.info("Ledger with name {} does not exist", ledgerName);
      return false;
    }
  }

  public void waitForActive(final String ledgerName) {
    LOG.info("Waiting for ledger with name: {} to become active", ledgerName);
    while (true) {
      Object waitObj = new Object();
      synchronized (waitObj) {
        if (checkLedgerActive(ledgerName)) {
          return;
        } else {
          try {
            waitObj.wait(Constants.DEFAULT_POLL_INTERVAL_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  public void deleteLedger(final String ledgerName) {
    if (ledgerExists(ledgerName)) {
      LOG.info("Attempting to delete ledger with name {}", ledgerName);
      DeleteLedgerRequest request = new DeleteLedgerRequest().withName(ledgerName);
      client.deleteLedger(request);
      LOG.info("Delete ledger with name: {}", ledgerName);
    }
  }

  public void waitForDeleted(final String ledgerName) {
    if ( !ledgerExists(ledgerName)) {
      return;
    }
    LOG.info("Waiting for ledger with name {} to be deleted", ledgerName);
    while (ledgerExists(ledgerName)) {
      Object waitObj = new Object();
      synchronized (waitObj) {
        if (checkLedgerActive(ledgerName)) {
          return;
        } else {
          try {
            waitObj.wait(Constants.DEFAULT_POLL_INTERVAL_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  public void createTable(final TransactionExecutor txn, final String tableName) {
    LOG.info("Creating table with name {}", tableName);
    final String createTable = String.format("create table %s", tableName);
    txn.execute(createTable);
    LOG.info("Created table with name {} successfully", tableName);
    return;
  }

  public void waitForTable(PooledQldbDriver driver, String tableName) {
    QldbSession session = driver.getSession();
    List<Object> tableExistsCond = new ArrayList<>();
    while (tableExistsCond.size() == 0) {
      session.execute(txn-> {
        if (tableExists(txn, tableName)) {
          tableExistsCond.add(new Object());
        }
      }, (retryAttempt) -> {
        LOG.warn("OCC Conflict while checking if table exists");
      });
      try {
        Thread.sleep(1_000L);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while sleeping");
        throw new RuntimeException(e);
      }
    }
    session.close();
  }

  public boolean tableExists(final TransactionExecutor txn, final String tableName) {
    LOG.info(String.format("Checking if table %s exists yet", tableName));
    final String query = String.format("select o.* from %s where id = ?", tableName);
    try {
      final List<IonValue> params = Collections
          .singletonList(Constants.MAPPER.writeValueAsIonValue("hfdskhkdsaflkjdahskljf"));
      final Result r = txn.execute(query, params);
      LOG.info("Check query returned {}", r.isEmpty());
      return true;
    } catch (IOException e) {
      LOG.error("Exception writing IonValue, shouldn't happen");
      throw new RuntimeException(e);
    } catch (BadRequestException t) {
      LOG.warn("Check query threw BadRequestException still waiting for table");
      return false;
    }
  }
}

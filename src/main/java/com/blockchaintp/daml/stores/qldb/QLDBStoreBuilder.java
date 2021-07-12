package com.blockchaintp.daml.stores.qldb;

import com.amazon.ion.IonSystem;
import software.amazon.qldb.QldbDriver;

public class QLDBStoreBuilder {

  private String table;
  private IonSystem ionSystem;
  private QldbDriver driver;
  private String ledger;

  private QLDBStoreBuilder(QldbDriver driver) {
    this.driver = driver;
  }

  public static QLDBStoreBuilder forDriver(QldbDriver driver) {
    return new QLDBStoreBuilder(driver);
  }

  public QLDBStoreBuilder forIonSystem(IonSystem system) {
    this.ionSystem = system;
    return this;
  }

  public QLDBStoreBuilder ledgerName(String ledger) {
    this.ledger = ledger;
    return this;
  }

  public QLDBStoreBuilder tableName(String table) {
    this.table = table;
    return this;
  }

  public QLDBStore build() throws QLDBStoreBuilderException {
    if (table == null) {
      throw new QLDBStoreBuilderException("No table name specfified in builder");
    }
    return new QLDBStore(
      driver,
      ledger,
      table,
      ionSystem
    );
  }
}

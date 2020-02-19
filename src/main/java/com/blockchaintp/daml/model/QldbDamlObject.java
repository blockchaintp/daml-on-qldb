package com.blockchaintp.daml.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonText;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.Constants;
import com.blockchaintp.daml.DamlLedger;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;

public abstract class QldbDamlObject implements DamlKeyValueRow {

  private static final Logger LOG = LoggerFactory.getLogger(QldbDamlObject.class);

  private final DamlLedger ledger;

  private String id;
  private String s3Key;

  protected transient byte[] s3data;

  private transient boolean hollow;

  public QldbDamlObject(final DamlLedger newLedger, @JsonProperty("id") final String newId,
      @JsonProperty("s3Key") final String newS3key, final byte[] newData) {
    this.ledger = newLedger;
    this.id = newId;
    this.s3Key = newS3key;
    this.s3data = newData;
    this.hollow = true;
  }

  public QldbDamlObject(final DamlLedger newLedger, @JsonProperty("id") final String newId, final byte[] newData) {
    this(newLedger, newId, Utils.hash512(newData), newData);
  }

  public QldbDamlObject(final DamlLedger newLedger, @JsonProperty("id") final String newId) {
    this(newLedger, newId, null, null);
  }

  protected DamlLedger getLedger() {
    return this.ledger;
  }

  @Override
  @JsonProperty("id")
  public String getId() {
    return this.id;
  }

  @Override
  @JsonProperty("id")
  public void setId(final String newId) {
    this.id = newId;
  }

  @JsonProperty("s3Key")
  public String getS3Key() {
    return this.s3Key;
  }

  @Override
  @JsonProperty("s3Key")
  public void setS3Key(final String newS3Key) {
    this.s3Key = newS3Key;
  }

  @Override
  public byte[] s3Data() {
    return this.s3data;
  }

  @Override
  public void refreshFromBulkStore() {
    this.s3data = getLedger().getObject(getS3Key(), true);
    LOG.info("Loaded {} bytes from bulk store", this.s3data.length);
  }

  @Override
  public void updateBulkStore() throws IOException {
    if (this.s3data == null) {
      throw new IOException(String.format("s3data null on insert for id=%s", getId()));
    }
    getLedger().putObject(getS3Key(), s3Data());
    LOG.info("Saved {} bytes to bulk store",this.s3data.length);
  }

  @Override
  public Result insert(final TransactionExecutor txn) throws IOException {
    LOG.info("insert id={} in table={}", getId(), tableName());

    final String query = String.format("insert into %s ?", tableName());
    LOG.info(String.format("QUERY = %s", query));
    final IonValue doc = Constants.MAPPER.writeValueAsIonValue(this);
    LOG.info("Inserting {}", doc.toPrettyString());
    final List<IonValue> params = Collections.singletonList(doc);
    return txn.execute(query, params);
  }

  @Override
  public Result update(final TransactionExecutor txn) throws IOException {
    LOG.info("update id={} in table={}", getId(), tableName());

    final String query = String.format("update %s set s3Key = ? where id = ?", tableName());
    LOG.info(String.format("QUERY = %s s3Key = %s ID = %s", query, getS3Key(), getId()));
    final List<IonValue> params = new ArrayList<>();
    params.add(Constants.MAPPER.writeValueAsIonValue(getS3Key()));
    params.add(Constants.MAPPER.writeValueAsIonValue(getId()));
    return txn.execute(query, params);
  }

  @Override
  public DamlKeyValueRow fetch(final TransactionExecutor txn) throws IOException {
    if (!hollow) {
      return this;
    }
    LOG.info("fetch id={} in table={}", getId(), tableName());

    final String query = String.format("select o.* from %s AS o where id = ?", tableName());
    final List<IonValue> params = Collections.singletonList(Constants.MAPPER.writeValueAsIonValue(getId()));
    final Result r = txn.execute(query, params);
    if (r.isEmpty()) {
      return this;
    } else {
      r.iterator().forEachRemaining(row -> {
        final IonStruct s = (IonStruct) row;
        final IonText v = (IonText) s.get("s3Key");
        this.s3Key = v.stringValue();
        this.hollow = false;
      });
      refreshFromBulkStore();
      return this;
    }
  }

  @Override
  public boolean exists(final TransactionExecutor txn) throws IOException {
    LOG.info("exists id={} in table={}", getId(), tableName());
    final String query = String.format("select o.* from %s AS o where id = ?", tableName());
    LOG.info(String.format("QUERY = %s ID = %s", query, getId()));
    final List<IonValue> params = Collections.singletonList(Constants.MAPPER.writeValueAsIonValue(getId()));
    final Result r = txn.execute(query, params);
    return !r.isEmpty();
  }

  @Override
  public boolean upsert(final TransactionExecutor txn) throws IOException {
    if (exists(txn)) {
      update(txn);
      return false;
    } else {
      insert(txn);
      return true;
    }
  }

  @Override
  public boolean delete(final TransactionExecutor txn) throws IOException {
    if (exists(txn)) {
      LOG.info("delete id={} in table={}", getId(), tableName());
      final String query = String.format("delete from %s where id = ?", tableName());
      LOG.info(String.format("QUERY = %s ID = %s", query, getId()));
      final List<IonValue> params = new ArrayList<>();
      params.add(Constants.MAPPER.writeValueAsIonValue(getId()));
      final Result r = txn.execute(query, params);
      if (r.isEmpty()) {
        this.hollow = true;
      } else {
        r.iterator().forEachRemaining(row -> {
          this.hollow = false;
        });
      }
      return true;
    } else {
      this.hollow = true;
      return false;
    }
  }
}

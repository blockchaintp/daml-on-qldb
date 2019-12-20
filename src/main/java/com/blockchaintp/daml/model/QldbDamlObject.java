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
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.qldb.Result;
import software.amazon.qldb.Transaction;
import software.amazon.qldb.Transaction;

public abstract class QldbDamlObject implements DamlKeyValueRow {

  private static final Logger LOG = LoggerFactory.getLogger(QldbDamlObject.class);

  private String id;
  private String s3Key;

  protected transient byte[] s3data;

  private transient boolean hollow;

  public QldbDamlObject(@JsonProperty("id") final String newId, @JsonProperty("s3Key") final String newS3key,
      final byte[] newData) {
    this.id = newId;
    this.s3Key = newS3key;
    this.s3data = newData;
    this.hollow = true;
  }

  public QldbDamlObject(@JsonProperty("id") final String newId) {
    this(newId, Utils.hash512(ByteString.copyFromUtf8(newId).toByteArray()), null);
  }

  public QldbDamlObject(@JsonProperty("id") final String newId, final byte[] newData) {
    this(newId, Utils.hash512(ByteString.copyFromUtf8(newId).toByteArray()), newData);
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
  public void refreshFromBulkStore(final DamlLedger ledger) {
    this.s3data = ledger.getObject(getS3Key());
    LOG.info("Loaded {} bytes from bulk store",this.s3data.length);
  }

  @Override
  public void updateBulkStore(final DamlLedger ledger) throws IOException {
    if (this.s3data == null) {
      throw new IOException(String.format("s3data null on insert for id=%s", getId()));
    }
    ledger.putObject(getS3Key(), s3Data());
    LOG.info("Saved {} bytes to bulk store",this.s3data.length);
  }

  @Override
  public Result insert(final Transaction txn, final DamlLedger ledger) throws IOException {
    LOG.info("insert id={} in table={}", getId(), tableName());

    final String query = String.format("insert into %s ?", tableName());
    final IonValue doc = Constants.MAPPER.writeValueAsIonValue(this);
    LOG.info("Inserting {}", doc.toPrettyString());
    final List<IonValue> params = Collections.singletonList(doc);
    return txn.execute(query, params);
  }

  @Override
  public Result update(final Transaction txn, final DamlLedger ledger) throws IOException {
    LOG.info("update id={} in table={}", getId(), tableName());

    final String query = String.format("update %s set s3Key = ? where id = ?", tableName());
    final List<IonValue> params = new ArrayList<>();
    params.add(Constants.MAPPER.writeValueAsIonValue(getS3Key()));
    params.add(Constants.MAPPER.writeValueAsIonValue(getId()));
    return txn.execute(query, params);
  }

  @Override
  public QldbDamlObject fetch(final Transaction txn, final DamlLedger ledger) throws IOException {
    if (!hollow) {
      return this;
    }
    LOG.info("fetch id={} in table={}", getId(), tableName());

    final String query = String.format("select * from %s where id = ?", tableName());
    final List<IonValue> params = new ArrayList<>();
    params.add(Constants.MAPPER.writeValueAsIonValue(getId()));
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
      refreshFromBulkStore(ledger);
      return this;
    }
  }

  @Override
  public boolean exists(final Transaction txn) throws IOException {
    LOG.info("exists id={} in table={}", getId(), tableName());
    final String query = String.format("select o from %s where id = ?", tableName());
    final List<IonValue> params = new ArrayList<>();
    params.add(Constants.MAPPER.writeValueAsIonValue(getId()));
    final Result r = txn.execute(query, params);
    return !r.isEmpty();
  }

  @Override
  public boolean upsert(final Transaction txn, final DamlLedger ledger) throws IOException {
    if (exists(txn)) {
      update(txn, ledger);
      return false;
    } else {
      insert(txn, ledger);
      return true;
    }
  }

  @Override
  public boolean delete(final Transaction txn, final DamlLedger ledger) throws IOException {
    if (exists(txn)) {
      LOG.info("delete id={} in table={}", getId(), tableName());
      final String query = String.format("delete from %s where id = ?", tableName());
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

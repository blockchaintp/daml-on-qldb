package com.blockchaintp.daml.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.Constants;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateValue;
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.ByteString;

import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;

public class QldbDamlState implements DamlKeyValueRow {

  public static final String TABLE_NAME = "kv_daml_state";
  private String entry;
  private final String entryId;

  private transient boolean hollow;

  public QldbDamlState(@JsonProperty("damlkey") String entryId, @JsonProperty("data") String entry) {
    this.entryId = entryId;
    this.entry = entry;
    this.hollow = false;
  }

  public QldbDamlState(@JsonProperty("damlkey") String entryId) {
    this.entryId = entryId;
    this.entry = null;
    this.hollow = true;
  }

  public static QldbDamlState create(DamlStateKey pbEntryId, DamlStateValue pbEntry) {
    String _entryId = KeyValueCommitting.packDamlStateKey(pbEntryId).toStringUtf8();
    String _entry = KeyValueCommitting.packDamlStateValue(pbEntry).toStringUtf8();
    return new QldbDamlState(_entryId, _entry);
  }

  @JsonProperty("damlkey")
  public String getEntryId() {
    return entryId;
  }

  @JsonProperty("data")
  public String getEntry() {
    return entry;
  }

  @Override
  public String damlkey() {
    return this.entryId;
  }

  @Override
  public String data() {
    return this.entry;
  }

  public DamlStateKey damlStateKey() {
    return KeyValueCommitting.unpackDamlStateKey(ByteString.copyFromUtf8(this.entryId));
  }

  public DamlStateValue damlStateValue() {
    return KeyValueCommitting.unpackDamlStateValue(ByteString.copyFromUtf8(this.entry));
  }

  @Override
  public Result insert(TransactionExecutor txn) throws IOException {
    final String query = String.format("insert into %s ?", TABLE_NAME);
    final IonValue doc = Constants.MAPPER.writeValueAsIonValue(this);
    final List<IonValue> params = Collections.singletonList(doc);
    return txn.execute(query, params);
  }

  @Override
  public Result update(TransactionExecutor txn) throws IOException {
    final String query = String.format("update %s set data = ? where damlkey = ?", TABLE_NAME);
    final List<IonValue> params = new ArrayList<>();
    params.add(Constants.MAPPER.writeValueAsIonValue(data()));
    params.add(Constants.MAPPER.writeValueAsIonValue(damlkey()));
    return txn.execute(query, params);
  }

  @Override
  public QldbDamlState fetch(TransactionExecutor txn) throws IOException {
    if (!hollow) {
      return this;
    }
    final String query = String.format("select o from %s where damlkey = ?", TABLE_NAME);
    final List<IonValue> params = new ArrayList<>();
    params.add(Constants.MAPPER.writeValueAsIonValue(damlkey()));
    Result r = txn.execute(query, params);
    if (r.isEmpty()) {
      return this;
    } else {
      r.iterator().forEachRemaining(row -> {
        IonStruct s = (IonStruct) row;
        IonValue v = s.get("data");
        this.entry = v.toString();
        this.hollow = false;
      });
      return this;
    }
  }

  @Override
  public boolean exists(TransactionExecutor txn) throws IOException {
    final String query = String.format("select o from %s where damlkey = ?", TABLE_NAME);
    final List<IonValue> params = new ArrayList<>();
    params.add(Constants.MAPPER.writeValueAsIonValue(damlkey()));
    Result r = txn.execute(query, params);
    if (r.isEmpty()) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public boolean upsert(TransactionExecutor txn) throws IOException {
    if (exists(txn)) {
      update(txn);
      return false;
    } else {
      insert(txn);
      return true;
    }
  }

  @Override
  public boolean delete(TransactionExecutor txn) throws IOException {
    if (exists(txn)) {
      final String query = String.format("delete from %s where damlkey = ?", TABLE_NAME);
      final List<IonValue> params = new ArrayList<>();
      params.add(Constants.MAPPER.writeValueAsIonValue(damlkey()));
      Result r = txn.execute(query, params);
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

  @Override
  public String tableName() {
    return TABLE_NAME;
  }
}

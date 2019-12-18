package com.blockchaintp.daml.model;

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.Constants;
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.ByteString;

import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;

public class QldbDamlLogEntry implements DamlKeyValueRow {

  public static final String TABLE_NAME = "kv_daml_log";
  private String entry;
  private final String entryId;

  private transient boolean hollow;
  private long offset;

  public QldbDamlLogEntry(@JsonProperty("damlkey") String entryId, @JsonProperty("data") String entry,
      @JsonProperty("offset") long offset) {
    this.entryId = entryId;
    this.entry = entry;
    this.offset = offset;
    this.hollow = false;
  }

  public QldbDamlLogEntry(@JsonProperty("damlkey") String entryId, @JsonProperty("data") String entry) {
    this.entryId = entryId;
    this.entry = entry;
    this.offset = -1L;
    this.hollow = true;
  }

  public QldbDamlLogEntry(@JsonProperty("damlkey") String entryId) {
    this.entryId = entryId;
    this.entry = null;
    this.offset = -1L;
    this.hollow = true;
  }

  public static QldbDamlLogEntry create(DamlLogEntryId pbEntryId, DamlLogEntry pbEntry) {
    String _entryId = KeyValueCommitting.packDamlLogEntryId(pbEntryId).toStringUtf8();
    String _entry = KeyValueCommitting.packDamlLogEntry(pbEntry).toStringUtf8();
    return new QldbDamlLogEntry(_entryId, _entry);
  }

  @JsonProperty("damlkey")
  public String getEntryId() {
    return entryId;
  }

  @JsonProperty("data")
  public String getEntry() {
    return entry;
  }

  /**
   * @return the offset
   */
  @JsonProperty("offset")
  public long getOffset() {
    return offset;
  }

  @Override
  public String damlkey() {
    return this.entryId;
  }

  @Override
  public String data() {
    return this.entry;
  }

  public DamlLogEntryId damlLogEntryId() {
    return KeyValueCommitting.unpackDamlLogEntryId(ByteString.copyFromUtf8(this.entryId));
  }

  public DamlLogEntry damlLogEntry() {
    return KeyValueCommitting.unpackDamlLogEntry(ByteString.copyFromUtf8(this.entry));
  }

  public static long getMaxOffset(TransactionExecutor txn) {
    final String query = String.format("select max(offset) from %s", TABLE_NAME);
    Result r = txn.execute(query);
    if (r.isEmpty()) {
      return -1L;
    } else {
      long offset = -1L;
      Iterator<IonValue> iter = r.iterator();
      while (iter.hasNext()) {
        IonValue row = iter.next();
        IonStruct s = (IonStruct) row;
        return Long.valueOf(s.get("_1").toString());
      }
      return offset;
    }
  }

  public static QldbDamlLogEntry getNextLogEntry(TransactionExecutor txn, long currentOffset) throws IOException {
    long nextOffset = currentOffset;
    if (currentOffset < 0) {
      nextOffset = 0;
    }
    final String query = String.format("select o from %s where offset = ?", TABLE_NAME);
    final List<IonValue> params = new ArrayList<>();
    params.add(Constants.MAPPER.writeValueAsIonValue(nextOffset));
    Result r = txn.execute(query, params);
    if (r.isEmpty()) {
      return null;
    } else {
      Iterator<IonValue> iter = r.iterator();
      if (iter.hasNext()) {
        IonValue row = iter.next();
        IonStruct s = (IonStruct) row;
        QldbDamlLogEntry e = new QldbDamlLogEntry(s.get("damlkey").toString(), s.get("data").toString(), Long.valueOf(s.get("offset").toString()));
        return e;
      } else {
        return null;
      }
    }
  }

  @Override
  public Result insert(TransactionExecutor txn) throws IOException {
    long currentOffset = getMaxOffset(txn);
    if (currentOffset == -1L) {
      currentOffset = 0;
    } else {
      currentOffset++;
    }
    final String query = String.format("insert into %s ?", TABLE_NAME);
    this.offset = currentOffset;
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
  public QldbDamlLogEntry fetch(TransactionExecutor txn) throws IOException {
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
        this.offset = Long.valueOf(s.get("offset").toString());
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

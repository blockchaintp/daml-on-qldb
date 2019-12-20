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
import com.blockchaintp.daml.DamlLedger;
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.qldb.Result;
import software.amazon.qldb.Transaction;

public class QldbDamlLogEntry implements DamlKeyValueRow {

  private static final Logger LOG = LoggerFactory.getLogger(QldbDamlLogEntry.class);
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
  public String s3Key() {
    return Utils.hash512(damlLogEntryId().toByteArray());
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

  public static long getMaxOffset(Transaction txn) {
    LOG.info("fetch maxOffset");

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
        if (!s.get("_1").isNullValue()) {
          offset = Long.valueOf(s.get("_1").toString());
        }
      }
      return offset;
    }
  }

  public static QldbDamlLogEntry getNextLogEntry(Transaction txn, long currentOffset) throws IOException {
    LOG.info("getNextLogEntry currentOffset={} in table={}", currentOffset, TABLE_NAME);
    long nextOffset = currentOffset;
    if (currentOffset < 0) {
      nextOffset = 0;
    }
    final String query = String.format("select o from %s where offset = ?", TABLE_NAME);
    final List<IonValue> params = new ArrayList<>();
    params.add(Constants.MAPPER.writeValueAsIonValue(nextOffset));
    Result r = txn.execute(query, params);
    if (r.isEmpty()) {
      LOG.info("No current log entries");
      return null;
    } else {
      Iterator<IonValue> iter = r.iterator();
      if (iter.hasNext()) {
        IonValue row = iter.next();
        IonStruct s = (IonStruct) row;
        QldbDamlLogEntry e = new QldbDamlLogEntry(s.get("damlkey").toString(), s.get("data").toString(), Long.valueOf(s.get("offset").toString()));
        return e;
      } else {
        LOG.info("No current log entries on iterator");
        return null;
      }
    }
  }

  @Override
  public Result insert(Transaction txn, DamlLedger ledger) throws IOException {
    long currentOffset = getMaxOffset(txn);
    if (currentOffset == -1L) {
      currentOffset = 0;
    } else {
      currentOffset++;
    }
    LOG.info("insert damlkey={} in table={}", damlkey(), TABLE_NAME);

    final String query = String.format("insert into %s ?", TABLE_NAME);
    this.offset = currentOffset;
    final IonValue doc = Constants.MAPPER.writeValueAsIonValue(this);
    final List<IonValue> params = Collections.singletonList(doc);
    return txn.execute(query, params);
  }

  @Override
  public Result update(Transaction txn, DamlLedger ledger) throws IOException {
    LOG.info("update damlkey={} in table={}", damlkey(), TABLE_NAME);

    final String query = String.format("update %s set data = ? where damlkey = ?", TABLE_NAME);
    final List<IonValue> params = new ArrayList<>();
    params.add(Constants.MAPPER.writeValueAsIonValue(data()));
    params.add(Constants.MAPPER.writeValueAsIonValue(damlkey()));
    return txn.execute(query, params);
  }

  @Override
  public QldbDamlLogEntry fetch(Transaction txn, DamlLedger ledger) throws IOException {
    if (!hollow) {
      return this;
    }
    LOG.info("fetch damlkey={} in table={}", damlkey(), TABLE_NAME);

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
  public boolean exists(Transaction txn) throws IOException {
    LOG.info("exists damlkey={} in table={}", damlkey(), TABLE_NAME);

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
  public boolean upsert(Transaction txn, DamlLedger ledger) throws IOException {
    if (exists(txn)) {
      update(txn,ledger);
      return false;
    } else {
      insert(txn,ledger);
      return true;
    }
  }

  @Override
  public boolean delete(Transaction txn, DamlLedger ledger) throws IOException {
    if (exists(txn)) {
      LOG.info("delete damlkey={} in table={}", damlkey(), TABLE_NAME);

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

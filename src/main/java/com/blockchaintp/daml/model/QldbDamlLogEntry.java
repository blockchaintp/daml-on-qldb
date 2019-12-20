package com.blockchaintp.daml.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonText;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.Constants;
import com.blockchaintp.daml.DamlLedger;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId;
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.qldb.Result;
import software.amazon.qldb.Transaction;

public class QldbDamlLogEntry extends QldbDamlObject {

  private static final Logger LOG = LoggerFactory.getLogger(QldbDamlLogEntry.class);
  public static final String TABLE_NAME = "kv_daml_log";
  private long offset;

  public QldbDamlLogEntry(@JsonProperty("id") final String newId, @JsonProperty("s3Key") final String newS3Key,
      @JsonProperty("offset") final long newOffset, final byte[] newData) {
    super(newId, newS3Key, newData);
    this.offset = newOffset;
  }

  public QldbDamlLogEntry(@JsonProperty("id") final String newId, final byte[] newData) {
    super(newId, newData);
    this.offset = -1L;
  }

  public QldbDamlLogEntry(@JsonProperty("id") final String newId) {
    super(newId);
    this.offset = -1L;
  }

  public QldbDamlLogEntry(@JsonProperty("id") final String newId, @JsonProperty("s3Key") final String newS3Key,
      @JsonProperty("offset") final long newOffset) {
    super(newId, newS3Key, null);
    this.offset = newOffset;
  }

  public QldbDamlLogEntry(@JsonProperty("id") final String newId, @JsonProperty("offset") final long newOffset) {
    super(newId);
    this.offset = newOffset;
  }

  public static QldbDamlLogEntry create(final DamlLogEntryId pbEntryId, final DamlLogEntry pbEntry) {
    final String packedId = KeyValueCommitting.packDamlLogEntryId(pbEntryId).toStringUtf8();
    final byte[] data = KeyValueCommitting.packDamlLogEntry(pbEntry).toByteArray();
    return new QldbDamlLogEntry(packedId, data);
  }

  /**
   * @return the offset
   */
  @JsonProperty("offset")
  public long getOffset() {
    return offset;
  }

  public DamlLogEntryId damlLogEntryId() {
    return KeyValueCommitting.unpackDamlLogEntryId(ByteString.copyFromUtf8(getId()));
  }

  public DamlLogEntry damlLogEntry() {
    return KeyValueCommitting.unpackDamlLogEntry(ByteString.copyFrom(s3Data()));
  }

  public static long getMaxOffset(final Transaction txn) {
    LOG.info("fetch maxOffset");

    final String query = String.format("select max(offset) from %s", TABLE_NAME);
    final Result r = txn.execute(query);
    if (r.isEmpty()) {
      return -1L;
    } else {
      long offset = -1L;
      final Iterator<IonValue> iter = r.iterator();
      while (iter.hasNext()) {
        final IonValue row = iter.next();
        final IonStruct s = (IonStruct) row;
        if (!s.get("_1").isNullValue()) {
          offset = Long.valueOf(s.get("_1").toString());
        }
        LOG.info("iterating");
      }
      LOG.info("Current max offset is {}", offset);
      return offset;
    }
  }

  public static QldbDamlLogEntry getNextLogEntry(final Transaction txn, final DamlLedger ledger, final long currentOffset)
      throws IOException {
    LOG.info("getNextLogEntry currentOffset={} in table={}", currentOffset, QldbDamlLogEntry.TABLE_NAME);
    long nextOffset = currentOffset;
    if (currentOffset < 0) {
      nextOffset = 0;
    } else {
      nextOffset = currentOffset + 1;
    }
    final String query = String.format("select * from %s where offset = ?", QldbDamlLogEntry.TABLE_NAME);
    final List<IonValue> params = new ArrayList<>();
    params.add(Constants.MAPPER.writeValueAsIonValue(nextOffset));
    final Result r = txn.execute(query, params);
    if (r.isEmpty()) {
      LOG.info("No current log entries");
      return null;
    } else {
      final Iterator<IonValue> iter = r.iterator();
      if (iter.hasNext()) {
        final IonValue row = iter.next();
        final IonStruct s = (IonStruct) row;
        LOG.info("got next log entry {}", s.toPrettyString());
        IonText idVal= (IonText) s.get("id");
        IonText s3KeyVal= (IonText) s.get("s3Key");
        final QldbDamlLogEntry e = new QldbDamlLogEntry(idVal.stringValue(), s3KeyVal.stringValue(),
            Long.valueOf(s.get("offset").toString()));
        e.refreshFromBulkStore(ledger);
        return e;
      } else {
        LOG.info("No current log entries on iterator");
        return null;
      }
    }
  }

  @Override
  public Result insert(final Transaction txn, final DamlLedger ledger) throws IOException {
    final long currentOffset = getMaxOffset(txn);
    if (currentOffset == -1L) {
      this.offset = 0;
    } else {
      this.offset = currentOffset + 1;
    }
    return super.insert(txn, ledger);
  }

  @Override
  public String tableName() {
    return QldbDamlLogEntry.TABLE_NAME;
  }
}

package com.blockchaintp.daml.model;

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId;
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.ByteString;

public class QldbDamlLogEntry {

  private final String entry;
  private final String entryId;

  public QldbDamlLogEntry(@JsonProperty("entryId_bs") String entryId, @JsonProperty("entry_bs") String entry) {
    this.entryId = entryId;
    this.entry = entry;
  }

  public static QldbDamlLogEntry create(DamlLogEntryId pbEntryId, DamlLogEntry pbEntry) {
    String _entryId = KeyValueCommitting.packDamlLogEntryId(pbEntryId).toStringUtf8();
    String _entry = KeyValueCommitting.packDamlLogEntry(pbEntry).toStringUtf8();
    return new QldbDamlLogEntry(_entryId, _entry);
  }

  @JsonProperty("entryId_bs")
  public String getEntryId() {
    return entryId;
  }

  @JsonProperty("entry_bs")
  public String getEntry() {
    return entry;
  }

  public DamlLogEntryId pbEntryId() {
    ByteString bs = ByteString.copyFromUtf8(this.entryId);
    return KeyValueCommitting.unpackDamlLogEntryId(bs);
  }

  public DamlLogEntry pbEntry() {
    ByteString bs = ByteString.copyFromUtf8(this.entry);
    return KeyValueCommitting.unpackDamlLogEntry(bs);
  }
}

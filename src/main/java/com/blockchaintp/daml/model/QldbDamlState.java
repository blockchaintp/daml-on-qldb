package com.blockchaintp.daml.model;

import com.amazonaws.util.Base64;
import com.blockchaintp.daml.DamlLedger;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateValue;
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.ByteString;

public final class QldbDamlState extends QldbDamlObject {

  public static final String TABLE_NAME = "kv_daml_state";

  public QldbDamlState(final DamlLedger targetLedger, @JsonProperty("id") final String newId,
      @JsonProperty("s3Key") final String newS3key, final byte[] data) {
    super(targetLedger, newId, newS3key, data);
  }

  public QldbDamlState(final DamlLedger targetLedger, @JsonProperty("id") final String id, final byte[] data) {
    super(targetLedger, id, data);
  }

  public QldbDamlState(final DamlLedger targetLedger, @JsonProperty("id") final String newId) {
    super(targetLedger, newId);
  }

  public static QldbDamlState create(final DamlLedger targetLedger, final DamlStateKey pbEntryId,
      final DamlStateValue pbEntry) {
    final String packedId = Base64.encodeAsString(KeyValueCommitting.packDamlStateKey(pbEntryId).toByteArray());

    final byte[] data = KeyValueCommitting.packDamlStateValue(pbEntry).toByteArray();
    return new QldbDamlState(targetLedger, packedId, data);
  }

  public DamlStateKey damlStateKey() {
    return KeyValueCommitting.unpackDamlStateKey(ByteString.copyFrom(Base64.decode(getId())));
  }

  public DamlStateValue damlStateValue() {
    return KeyValueCommitting.unpackDamlStateValue(ByteString.copyFrom(s3Data()));
  }

  @Override
  public String tableName() {
    return QldbDamlState.TABLE_NAME;
  }
}

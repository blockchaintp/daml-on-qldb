/*
 * Copyright 2021 Blockchain Technology Partners
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.blockchaintp.daml.stores.qldb;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import com.amazon.ion.IonInt;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.google.protobuf.ByteString;

import io.vavr.Tuple;
import software.amazon.awssdk.services.qldb.model.QldbException;
import software.amazon.awssdk.services.qldbsession.model.QldbSessionException;
import software.amazon.qldb.ExecutorNoReturn;
import software.amazon.qldb.QldbDriver;

/**
 * Implements a transaction log using 2 QLDB tables.
 *
 * daml_tx_log: i: UUID as a blob, indexed v: transaction value
 *
 * daml_tx_seq: s: Long sequence field, indexed d: docid of a daml_tx_log entry
 */
public final class QldbTransactionLog implements TransactionLog<UUID, ByteString, Long> {
  private static final String ID_FIELD = "i";
  private static final String SEQ_FIELD = "s";
  private static final String DOCID_FIELD = "d";
  private static final String DATA_FIELD = "v";
  private static final int UUID_LENGTH_IN_BYTES = 16;
  private final RequiresTables tables;
  private final String txLogTable;
  private final String seqTable;
  private final QldbDriver driver;
  private final IonSystem ion;
  private QldbTxSeq seqSource;

  /**
   * Construct a new QLDB transaction log for an id, sequence and opaque blob value.
   *
   * @param tableName
   *          A common prefix for the transaction log tables names
   * @param theDriver
   * @param ionSystem
   */
  public QldbTransactionLog(final String tableName, final QldbDriver theDriver, final IonSystem ionSystem) {
    this.driver = theDriver;
    this.ion = ionSystem;
    this.txLogTable = String.format("%s_tx_log", tableName);
    this.seqTable = String.format("%s_seq", tableName);
    this.tables = new RequiresTables(Arrays.asList(Tuple.of(txLogTable, ID_FIELD), Tuple.of(seqTable, SEQ_FIELD)),
        theDriver);
  }

  private static UUID asUuid(final byte[] bytes) {
    var bb = ByteBuffer.wrap(bytes);
    var firstLong = bb.getLong();
    var secondLong = bb.getLong();
    return new UUID(firstLong, secondLong);
  }

  private static byte[] asBytes(final UUID uuid) {
    var bb = ByteBuffer.wrap(new byte[UUID_LENGTH_IN_BYTES]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }

  @Override
  public UUID begin() throws StoreWriteException {
    var uuid = UUID.randomUUID();
    try {
      var uuidBytes = asBytes(uuid);

      driver.execute(tx -> {
        var struct = ion.newEmptyStruct();
        struct.add(ID_FIELD, ion.newBlob(uuidBytes));
        tx.execute(String.format("insert into %s value ?", txLogTable), struct);
      });
    } catch (QldbException e) {
      throw new StoreWriteException(e);
    }
    return uuid;
  }

  @Override
  public void sendEvent(final UUID id, final ByteString data) throws StoreWriteException {
    try {
      var uuidBytes = asBytes(id);

      driver.execute(tx -> {
        var struct = ion.newEmptyStruct();
        struct.add(ID_FIELD, ion.newBlob(uuidBytes));
        tx.execute(String.format("update %s as o set %s = ? where o.%s = ?", txLogTable, DATA_FIELD, ID_FIELD),
            ion.newBlob(data.toByteArray()), ion.newBlob(uuidBytes));
      });
    } catch (QldbException e) {
      throw new StoreWriteException(e);
    }
  }

  private void ensureSequence() throws QldbSessionException {
    if (seqSource != null) {
      return;
    }

    driver.execute(tx -> {
      var res = tx.execute(String.format("select max(%s) from %s", SEQ_FIELD, seqTable));
      if (res.isEmpty()) {
        this.seqSource = new QldbTxSeq(0L);
      } else {
        var s = (IonStruct) res.iterator().next();
        var i = (IonInt) s.get("_1");
        if (i == null) {
          throw QldbSessionException.create("", QldbTransactionException.invalidSchema(s));
        }

        this.seqSource = new QldbTxSeq(i.longValue());
      }
    });
  }

  @Override
  public Long commit(final UUID txId) throws StoreWriteException {
    try {
      ensureSequence();

      var uuidBytes = asBytes(txId);

      driver.execute((ExecutorNoReturn) tx -> {
        var query = String.format("select metadata.id from _ql_committed_%s as o where o.%s = ?", txLogTable, ID_FIELD);
        var r = tx.execute(query, ion.newBlob(uuidBytes));

        if (r.isEmpty()) {
          throw QldbException.create("", QldbTransactionException.noMetadata(query));
        }

        var metaData = (IonStruct) r.iterator().next();
        var docid = (IonString) metaData.get("id");

        if (docid == null) {
          throw QldbException.create("", QldbTransactionException.invalidSchema(metaData));
        }

        var struct = ion.newEmptyStruct();
        struct.add(SEQ_FIELD, ion.newInt(seqSource.peekNext()));
        struct.add(DOCID_FIELD, docid);

        tx.execute(String.format("insert into %s value ?", txLogTable, DATA_FIELD, ID_FIELD), struct);
      });
    } catch (QldbException e) {
      throw new StoreWriteException(e);
    }
    return seqSource.takeNext();
  }

  @Override
  public void abort(final UUID txId) {
  }
}

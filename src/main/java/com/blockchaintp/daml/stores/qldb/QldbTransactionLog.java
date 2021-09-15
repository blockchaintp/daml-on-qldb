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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonNull;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.utility.UuidConverter;
import com.google.protobuf.ByteString;

import static software.amazon.awssdk.services.qldbsession.model.QldbSessionException.create;
import io.vavr.API;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
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
  private static final Long PAGE_SIZE = 100L;
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(QldbTransactionLog.class);
  private static final String ID_FIELD = "i";
  private static final String COMPLETE_FIELD = "c";
  private static final String SEQ_FIELD = "s";
  private static final String DOCID_FIELD = "d";
  private static final String DATA_FIELD = "v";
  private final RequiresTables tables;
  private final String txLogTable;
  private final String seqTable;
  private final QldbDriver driver;
  private final IonSystem ion;
  private QldbTxSeq seqSource;

  /**
   *
   * @param driver
   * @return A QldbTransactionBuilder for the supplied driver.
   */
  public static QldbTransactionLogBuilder forDriver(final QldbDriver driver) {
    return new QldbTransactionLogBuilder(driver);
  }

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

  @Override
  public UUID begin() throws StoreWriteException {
    tables.checkTables();
    ensureSequence();
    var next = seqSource.takeNext();
    var uuid = UUID.randomUUID();
    LOG.info("Begin transaction {}", () -> uuid);
    var uuidBytes = UuidConverter.asBytes(uuid);

    try {
      driver.execute(tx -> {
        var struct = ion.newEmptyStruct();
        struct.add(ID_FIELD, ion.newBlob(uuidBytes));
        struct.add(COMPLETE_FIELD, ion.newBool(false));
        var query = String.format("insert into %s value ?", txLogTable);
        var r = tx.execute(query, struct);

        if (r.isEmpty()) {
          throw create("", QldbTransactionException.noMetadata(query));
        }

        var metaData = (IonStruct) r.iterator().next();
        var docid = metaData.get("documentId");

        if (docid == null || docid instanceof IonNull) {
          throw create("", QldbTransactionException.invalidSchema(metaData));
        }

        var seq = ion.newEmptyStruct();
        seq.add(SEQ_FIELD, ion.newInt(next));
        seq.add(DOCID_FIELD, ion.newString(((IonString) docid).stringValue()));

        tx.execute(String.format("insert into %s value ?", seqTable), seq);

      });
    } catch (QldbException e) {
      throw new StoreWriteException(e);
    }

    return uuid;
  }

  @Override
  public void sendEvent(final UUID id, final ByteString data) throws StoreWriteException {
    tables.checkTables();
    LOG.info("Send transaction event {}", () -> id);
    try {
      var uuidBytes = UuidConverter.asBytes(id);

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

  private Long ensureSequence() throws QldbSessionException {
    tables.checkTables();
    if (seqSource != null) {
      return seqSource.head();
    }

    return driver.execute(tx -> {
      var res = tx.execute(String.format("select max(%s) from %s", SEQ_FIELD, seqTable));
      if (res.isEmpty()) {
        this.seqSource = new QldbTxSeq(-1L);
      } else {
        var s = (IonStruct) res.iterator().next();

        var v = s.get("_1");

        if (v == null) {
          throw create("", QldbTransactionException.invalidSchema(s));
        }

        if (v instanceof IonNull) {
          LOG.info("No MAX seq found");
          this.seqSource = new QldbTxSeq(-1L);
        } else {
          var i = (IonInt) s.get("_1");
          LOG.info("MAX seq found {}", i::longValue);
          this.seqSource = new QldbTxSeq(i.longValue());

        }
      }
      return seqSource.head();
    });
  }

  @Override
  public Long commit(final UUID txId) throws StoreWriteException {
    tables.checkTables();
    LOG.info("Commit transaction {}", () -> txId);
    try {

      var uuidBytes = UuidConverter.asBytes(txId);

      return driver.execute(tx -> {
        tx.execute(String.format("update %s as o set %s = ? where o.%s = ?", txLogTable, COMPLETE_FIELD, ID_FIELD),
            ion.newBool(true), ion.newBlob(uuidBytes));

        var query = String.format("select s.%s from %s as s, %s as d BY d_id where d_id = s.%s and d.%s = ?", SEQ_FIELD,
            seqTable, txLogTable, DOCID_FIELD, ID_FIELD);

        LOG.debug("Query {}", query);

        var rx = tx.execute(query, ion.newBlob(uuidBytes));

        if (rx.isEmpty()) {
          throw create("", QldbTransactionException.noMetadata(query));
        }

        var res = rx.iterator().next();

        if (res instanceof IonStruct) {
          var ress = (IonStruct) res;

          var seq = ress.get(SEQ_FIELD);

          if (!(seq instanceof IonInt)) {
            throw create("", QldbTransactionException.invalidSchema(res));
          }

          return ((IonInt) seq).longValue();
        }

        throw create("", QldbTransactionException.invalidSchema(res));
      });
    } catch (QldbException e) {
      throw new StoreWriteException(e);
    }
  }

  @Override
  public void abort(final UUID txId) {
    tables.checkTables();

    driver.execute(
        (ExecutorNoReturn) tx -> tx.execute(String.format("delete from %s as o where o.%s = ?", txLogTable, ID_FIELD),
            ion.newBlob(UuidConverter.asBytes(txId))));
  }

  private Tuple2<Long, Map.Entry<UUID, ByteString>> fromResult(final IonValue result) throws QldbTransactionException {
    var s = (IonStruct) result;
    if (s == null) {
      throw QldbTransactionException.notAStruct(Objects.requireNonNull(result));
    }

    var idBytes = (IonBlob) s.get(ID_FIELD);
    var data = (IonBlob) s.get(DATA_FIELD);
    var seq = (IonInt) s.get(SEQ_FIELD);

    if (idBytes == null || data == null) {
      throw QldbTransactionException.invalidSchema(s);
    }

    return Tuple.of(seq.longValue(),
        Map.entry(UuidConverter.asUuid(idBytes.getBytes()), ByteString.copyFrom(data.getBytes())));
  }

  @Override
  public Stream<Tuple3<Long, UUID, ByteString>> from(final Long startExclusive, final Optional<Long> endInclusive)
      throws StoreReadException {
    LOG.info("From {}  to {}", startExclusive, endInclusive);
    tables.checkTables();

    var qP = "select s.%s,d.%s,d.%s from %s as d BY d_id, %s as s where d_id = s.%s and d.%s = true and s.%s in ( %s )";

    var rx = new Iterator<Tuple3<Long, UUID, ByteString>>() {
      private Long position = startExclusive;
      private Deque<Tuple3<Long, UUID, ByteString>> currentPage;

      private Deque<Tuple3<Long, UUID, ByteString>> nextPage(final LongStream range) {
        return driver.execute(tx -> {
          var ids = range.mapToObj((long x) -> String.format("%d", x)).collect(Collectors.joining(","));

          LOG.info("Querying for page ({})", () -> ids);

          if (ids.isEmpty()) {
            return new ArrayDeque<>();
          }
          var query = String.format(qP, SEQ_FIELD, ID_FIELD, DATA_FIELD, txLogTable, seqTable, DOCID_FIELD,
              COMPLETE_FIELD, SEQ_FIELD, ids);
          var r = tx.execute(query);

          return StreamSupport.stream(r.spliterator(), false).map(x -> API.unchecked(() -> fromResult(x)).get())
              .sorted(Comparator.comparingLong(x -> x._1)).map(x -> Tuple.of(x._1, x._2.getKey(), x._2.getValue()))
              .collect(Collectors.toCollection(ArrayDeque::new));
        });
      }

      @Override
      public boolean hasNext() {
        var toFetch = PAGE_SIZE;

        if (endInclusive.isPresent()) {
          toFetch = Math.min(PAGE_SIZE, endInclusive.get() - position);
        }

        if (currentPage == null || currentPage.isEmpty()) {
          currentPage = nextPage(LongStream.range(position + 1, position + toFetch + 1));
        }

        return !currentPage.isEmpty();
      }

      @Override
      public Tuple3<Long, UUID, ByteString> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        var next = currentPage.remove();

        position = next._1;

        return next;
      }

      @SuppressWarnings("EmptyMethod")
      @Override
      public void remove() {
        Iterator.super.remove();
      }

      @Override
      public void forEachRemaining(final Consumer<? super Tuple3<Long, UUID, ByteString>> action) {
        Iterator.super.forEachRemaining(action);
      }
    };

    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(rx,
        Spliterator.IMMUTABLE | Spliterator.DISTINCT | Spliterator.ORDERED | Spliterator.NONNULL), false);
  }

  @Override
  public Optional<Long> getLatestOffset() {
    var next = ensureSequence();
    if (next == -1) {
      return Optional.empty();
    }

    return Optional.of(ensureSequence() - 1);
  }
}

/*
 * Copyright © 2023 Paravela Limited
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
package com.blockshaintp.daml.stores.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.layers.WrapFunction0;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.google.protobuf.ByteString;

import io.vavr.CheckedFunction0;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;

/**
 * A transaction log implemented on postgres.
 */
public final class PostgresTransactionLog implements TransactionLog<UUID, ByteString, Long> {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(PostgresTransactionLog.class);
  private static final int ENVELOPE_IDX = 3;
  private static final int SEQ_IDX = 2;
  private static final int UUID_INDEX = 1;
  private final Connection connection;

  /**
   *
   * @param url
   * @return a builder for PostgresTransactionLog.
   */
  public static PostgresTransactionLogBuilder fromUrl(final String url) {
    return new PostgresTransactionLogBuilder(url);
  }

  /**
   *
   * @param theConnection
   */
  public PostgresTransactionLog(final Connection theConnection) {
    connection = theConnection;
  }

  @Override
  public Stream<Tuple3<Long, UUID, ByteString>> from(final Long startExclusive, final Optional<Long> endInclusive)
      throws StoreReadException {

    LOG.debug("Query from {} to {}", startExclusive, endInclusive);

    var stmt = endInclusive.map(end -> WrapFunction0.ofChecked(() -> {
      var s = connection.prepareStatement(
          "select id,seq,data from tx where seq > ? and ? >= seq and complete = true order by seq asc",
          ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      s.setLong(1, startExclusive);
      s.setLong(2, end);

      return s;
    }, StoreReadException::new).unchecked()).orElseGet(() -> WrapFunction0.ofChecked(() -> {
      var s = connection.prepareStatement("select id,seq,data from tx where ? < seq and complete = true order by seq",
          ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      s.setLong(1, startExclusive);

      return s;
    }, StoreReadException::new).unchecked());

    var resultSet = WrapFunction0.ofChecked(stmt::executeQuery, StoreReadException::new).apply();

    return StreamSupport.stream(new ResultSetSpliterator(resultSet), false)
        .map(rx -> closingWrap(rx, () -> Tuple.of(rx.getLong(SEQ_IDX), rx.getObject(UUID_INDEX, UUID.class),
            ByteString.copyFrom(rx.getBytes(ENVELOPE_IDX)))).unchecked());
  }

  /**
   * A function wrapper that will wrap thrown exceptions with StoreReadException and close the passed
   * resultset.
   *
   * @param rx
   * @param fn
   * @param <R>
   * @return
   */
  private <R> WrapFunction0<R, StoreReadException> closingWrap(final ResultSet rx, final CheckedFunction0<R> fn) {
    return WrapFunction0.ofChecked(fn, e -> {
      try {
        rx.close();
      } catch (SQLException e2) {
        return new StoreReadException(e2);
      }
      return new StoreReadException(e);
    });
  }

  @Override
  public Optional<Long> getLatestOffset() {
    try (var stmt = connection.prepareStatement(
        "select seq from tx where tx.complete = true order by tx.seq desc limit 1", ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY)) {
      var rx = stmt.executeQuery();

      if (!rx.first()) {
        return Optional.empty();
      }

      return Optional.of(rx.getLong(1));

    } catch (SQLException e) {
      LOG.error("Error {}", e);
    }

    return Optional.empty();
  }

  @Override
  public Tuple2<UUID, Long> begin(final Optional<UUID> id) throws StoreWriteException {
    try (var stmt = connection.prepareStatement(
        "insert into tx(id,data,complete) values (?,null,false) returning id,seq", ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY)) {

      if (id.isPresent()) {
        stmt.setObject(1, id.get());
      } else {
        stmt.setObject(1, UUID.randomUUID());
      }

      var rx = stmt.executeQuery();
      if (!rx.first()) {
        throw new StoreWriteException(PostgresTxLogException.noInsertResult());
      }

      return Tuple.of(rx.getObject(1, UUID.class), rx.getLong(2));
    } catch (SQLException e) {
      throw new StoreWriteException(e);
    }
  }

  @Override
  public void sendEvent(final UUID id, final ByteString data) throws StoreWriteException {
    try (var stmt = connection.prepareStatement("update tx set data = ? where tx.id = ? and tx.complete = false")) {
      stmt.setBytes(1, data.toByteArray());
      stmt.setObject(2, id);
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new StoreWriteException(e);
    }
  }

  @Override
  public Long commit(final UUID txId) throws StoreWriteException {
    try (
        var stmt = connection.prepareStatement(
            "update tx set complete = true, ts = ? where tx.id = ? and tx.complete = false",
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        var getSeq = connection.prepareStatement("select tx.seq from tx where tx.id = ?",
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);) {

      stmt.setTimestamp(1, Timestamp.from(Instant.now()));
      stmt.setObject(2, txId);

      stmt.executeUpdate();

      getSeq.setObject(1, txId);
      var rx = getSeq.executeQuery();

      if (!rx.first()) {
        throw new StoreWriteException(PostgresTxLogException.noInsertResult());
      }

      return rx.getLong(1);

    } catch (SQLException e) {
      throw new StoreWriteException(e);
    }
  }

  @Override
  public void abort(final UUID txId) throws StoreWriteException {
    // No real need for abort processing. Maybe this should be begin / commit transaction
  }
}

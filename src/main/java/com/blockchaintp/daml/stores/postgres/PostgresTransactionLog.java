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
package com.blockchaintp.daml.stores.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.layers.WrapFunction0;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.utility.UuidConverter;
import com.google.protobuf.ByteString;

import io.vavr.CheckedFunction0;
import io.vavr.Tuple;
import io.vavr.Tuple3;

/**
 * A transaction log implemented on postgres.
 */
public final class PostgresTransactionLog implements TransactionLog<UUID, ByteString, Long> {
  private static final int ENVELOPE_IDX = 3;
  private static final int SEQ_IDX = 2;
  private static final int UUID_INDEX = 1;
  private final Connection connection;

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

    var stmt = endInclusive.map(end -> WrapFunction0.ofChecked(() -> {
      var s = connection.prepareStatement("select id,seq,data from tx_log_committed where ? < seq <= ?",
          ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      s.setLong(1, startExclusive);
      s.setLong(2, end);

      return s;
    }, StoreReadException::new).unchecked()).orElseGet(() -> WrapFunction0.ofChecked(() -> {
      var s = connection.prepareStatement("select id,seq,data from tx_log_committed where ? < seq",
          ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      s.setLong(1, startExclusive);

      return s;
    }, StoreReadException::new).unchecked());

    var resultSet = WrapFunction0.ofChecked(() -> stmt.executeQuery(), StoreReadException::new).apply();

    return Stream.iterate(resultSet, rx -> closingWrap(rx, () -> rx.first() && rx.isFirst() || rx.next()).unchecked(),
        rx -> closingWrap(rx, () -> {
          rx.next();
          return rx;
        }).unchecked())
        .map(rx -> closingWrap(rx, () -> Tuple.of(rx.getLong(SEQ_IDX), UuidConverter.asUuid(rx.getBytes(UUID_INDEX)),
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
    return Optional.empty();
  }

  @Override
  public UUID begin() throws StoreWriteException {
    return null;
  }

  @Override
  public void sendEvent(final UUID id, final ByteString data) throws StoreWriteException {

  }

  @Override
  public Long commit(final UUID txId) throws StoreWriteException {
    return null;
  }

  @Override
  public void abort(final UUID txId) throws StoreWriteException {

  }
}

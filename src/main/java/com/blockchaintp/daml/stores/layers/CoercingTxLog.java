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
package com.blockchaintp.daml.stores.layers;

import java.util.Optional;
import java.util.function.Function;

import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.TransactionLogReader;
import com.blockchaintp.daml.stores.service.TransactionLogWriter;

import io.reactivex.rxjava3.core.Observable;
import io.vavr.Tuple;
import io.vavr.Tuple3;

/**
 * A transaction log with coercing bijections.
 *
 * @param <K1>
 *          The target identity type.
 * @param <K2>
 *          The source identity type.
 * @param <V1>
 *          The target value type.
 * @param <V2>
 *          The source value type.
 * @param <I1>
 *          The target sequence type.
 * @param <I2>
 *          The source sequence type.
 */
public final class CoercingTxLog<K1, K2, V1, V2, I1, I2> implements TransactionLog<K1, V1, I1> {
  private final Function<K2, K1> keyCoercionFrom;
  private final Function<V2, V1> valueCoercionFrom;
  private final Function<I2, I1> seqCoercionFrom;
  private final Function<K1, K2> keyCoercionTo;
  private final Function<V1, V2> valueCoercionTo;
  private final Function<I1, I2> seqCoercionTo;
  private final TransactionLog<K2, V2, I2> inner;

  /**
   * Convenience method for building a coercing transaction log.
   *
   * @param keyCoercionFrom
   * @param valueCoercionFrom
   * @param seqCoercionFrom
   * @param keyCoercionTo
   * @param valueCoercionTo
   * @param seqCoercionTo
   * @param inner
   * @param <KK1>
   * @param <KK2>
   * @param <VV1>
   * @param <VV2>
   * @param <II1>
   * @param <II2>
   * @return a wrapped, coercing transaction log.
   */
  public static <KK1, KK2, VV1, VV2, II1, II2> TransactionLog<KK1, VV1, II1> from(
      final Function<KK2, KK1> keyCoercionFrom, final Function<VV2, VV1> valueCoercionFrom,
      final Function<II2, II1> seqCoercionFrom, final Function<KK1, KK2> keyCoercionTo,
      final Function<VV1, VV2> valueCoercionTo, final Function<II1, II2> seqCoercionTo,
      final TransactionLog<KK2, VV2, II2> inner) {
    return new CoercingTxLog<>(keyCoercionFrom, valueCoercionFrom, seqCoercionFrom, keyCoercionTo, valueCoercionTo,
        seqCoercionTo, inner);
  }

  /**
   * Convenience method for building a coercing transaction log.
   *
   * @param keyCoercionFrom
   * @param valueCoercionFrom
   * @param seqCoercionFrom
   * @param keyCoercionTo
   * @param valueCoercionTo
   * @param seqCoercionTo
   * @param inner
   * @param <KK1>
   * @param <KK2>
   * @param <VV1>
   * @param <VV2>
   * @param <II1>
   * @param <II2>
   * @return a wrapped, coercing transaction log.
   */
  public static <KK1, KK2, VV1, VV2, II1, II2> TransactionLogReader<II1, KK1, VV1> readerFrom(
      final Function<KK2, KK1> keyCoercionFrom, final Function<VV2, VV1> valueCoercionFrom,
      final Function<II2, II1> seqCoercionFrom, final Function<KK1, KK2> keyCoercionTo,
      final Function<VV1, VV2> valueCoercionTo, final Function<II1, II2> seqCoercionTo,
      final TransactionLog<KK2, VV2, II2> inner) {
    return from(keyCoercionFrom, valueCoercionFrom, seqCoercionFrom, keyCoercionTo, valueCoercionTo, seqCoercionTo,
        inner);
  }

  /**
   * Convenience method for building a coercing transaction log.
   *
   * @param keyCoercionFrom
   * @param valueCoercionFrom
   * @param seqCoercionFrom
   * @param keyCoercionTo
   * @param valueCoercionTo
   * @param seqCoercionTo
   * @param inner
   * @param <KK1>
   * @param <KK2>
   * @param <VV1>
   * @param <VV2>
   * @param <II1>
   * @param <II2>
   * @return a wrapped, coercing transaction log.
   */
  public static <KK1, KK2, VV1, VV2, II1, II2> TransactionLogWriter<KK1, VV1, II1> writerFrom(
      final Function<KK2, KK1> keyCoercionFrom, final Function<VV2, VV1> valueCoercionFrom,
      final Function<II2, II1> seqCoercionFrom, final Function<KK1, KK2> keyCoercionTo,
      final Function<VV1, VV2> valueCoercionTo, final Function<II1, II2> seqCoercionTo,
      final TransactionLog<KK2, VV2, II2> inner) {
    return from(keyCoercionFrom, valueCoercionFrom, seqCoercionFrom, keyCoercionTo, valueCoercionTo, seqCoercionTo,
        inner);
  }

  /**
   * Wraps a transaction log with bijections to convert type parameters.
   *
   * @param theKeyCoercionFrom
   * @param theValueCoercionFrom
   * @param theSeqCoercionFrom
   * @param theKeyCoercionTo
   * @param theValueCoercionTo
   * @param theSeqCoercionTo
   * @param theInner
   */
  public CoercingTxLog(final Function<K2, K1> theKeyCoercionFrom, final Function<V2, V1> theValueCoercionFrom,
      final Function<I2, I1> theSeqCoercionFrom, final Function<K1, K2> theKeyCoercionTo,
      final Function<V1, V2> theValueCoercionTo, final Function<I1, I2> theSeqCoercionTo,
      final TransactionLog<K2, V2, I2> theInner) {
    keyCoercionFrom = theKeyCoercionFrom;
    valueCoercionFrom = theValueCoercionFrom;
    seqCoercionFrom = theSeqCoercionFrom;
    keyCoercionTo = theKeyCoercionTo;
    valueCoercionTo = theValueCoercionTo;
    seqCoercionTo = theSeqCoercionTo;
    inner = theInner;
  }

  @Override
  public Observable<Tuple3<I1, K1, V1>> from(final Optional<I1> offset) {
    return inner.from(offset.map(seqCoercionTo::apply))
        .map(r -> Tuple.of(seqCoercionFrom.apply(r._1), keyCoercionFrom.apply(r._2), valueCoercionFrom.apply(r._3)));
  }

  @Override
  public K1 begin() throws StoreWriteException {
    return keyCoercionFrom.apply(inner.begin());
  }

  @Override
  public void sendEvent(final K1 id, final V1 data) throws StoreWriteException {
    inner.sendEvent(keyCoercionTo.apply(id), valueCoercionTo.apply(data));
  }

  @Override
  public I1 commit(final K1 txId) throws StoreWriteException {
    return seqCoercionFrom.apply(inner.commit(keyCoercionTo.apply(txId)));
  }

  @Override
  public void abort(final K1 txId) throws StoreWriteException {
    inner.abort(keyCoercionTo.apply(txId));
  }
}

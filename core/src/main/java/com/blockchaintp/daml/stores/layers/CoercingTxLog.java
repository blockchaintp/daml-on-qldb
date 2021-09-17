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
import java.util.stream.Stream;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.TransactionLog;

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
  private final Bijection<K1, K2> keyCoercion;
  private final Bijection<V1, V2> valueCoercion;
  private final Bijection<I1, I2> seqCoercion;
  private final TransactionLog<K2, V2, I2> inner;

  /**
   * Convenience method for building a coercing transaction log.
   *
   * @param theKeyCoercion
   * @param theValueCoercion
   * @param theSeqCoercion
   * @param inner
   * @param <K3>
   * @param <K4>
   * @param <V3>
   * @param <V4>
   * @param <I3>
   * @param <I4>
   * @return a wrapped, coercing transaction log.
   */
  public static <K3, K4, V3, V4, I3, I4> TransactionLog<K3, V3, I3> from(final Bijection<K3, K4> theKeyCoercion,
      final Bijection<V3, V4> theValueCoercion, final Bijection<I3, I4> theSeqCoercion,
      final TransactionLog<K4, V4, I4> inner) {
    return new CoercingTxLog<>(theKeyCoercion, theValueCoercion, theSeqCoercion, inner);
  }

  /**
   * Wraps a transaction log with bijections to convert type parameters.
   *
   * @param theKeyCoercion
   * @param theValueCoercion
   * @param theSeqCoercion
   * @param theInner
   */
  public CoercingTxLog(final Bijection<K1, K2> theKeyCoercion, final Bijection<V1, V2> theValueCoercion,
      final Bijection<I1, I2> theSeqCoercion, final TransactionLog<K2, V2, I2> theInner) {
    keyCoercion = theKeyCoercion;
    valueCoercion = theValueCoercion;
    seqCoercion = theSeqCoercion;
    inner = theInner;
  }

  @Override
  public Stream<Tuple3<I1, K1, V1>> from(final I1 startExclusive,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<I1> endInclusive)
      throws StoreReadException {
    return inner.from(seqCoercion.to(startExclusive), endInclusive.map(seqCoercion::to))
        .map(r -> Tuple.of(seqCoercion.from(r._1), keyCoercion.from(r._2), valueCoercion.from(r._3)));
  }

  @Override
  public Optional<I1> getLatestOffset() {
    return inner.getLatestOffset().map(seqCoercion::from);
  }

  @Override
  public K1 begin(final Optional<K1> id) throws StoreWriteException {
    return keyCoercion.from(inner.begin(id.map(keyCoercion::to)));
  }

  @Override
  public void sendEvent(final K1 id, final V1 data) throws StoreWriteException {
    inner.sendEvent(keyCoercion.to(id), valueCoercion.to(data));
  }

  @Override
  public I1 commit(final K1 txId) throws StoreWriteException {
    return seqCoercion.from(inner.commit(keyCoercion.to(txId)));
  }

  @Override
  public void abort(final K1 txId) throws StoreWriteException {
    inner.abort(keyCoercion.to(txId));
  }
}

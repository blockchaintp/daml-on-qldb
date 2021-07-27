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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.blockchaintp.daml.stores.service.SeqSource;

/**
 * A long sequence, initialisable from a QLDB tx log sequence table or an explicit point.
 */
public final class QldbTxSeq implements SeqSource<Long> {
  /**
   * Marker for the start of a sequence.
   */
  private long current;

  /**
   * Initialise the sequence at a particular point.
   *
   * @param start
   */
  public QldbTxSeq(final Long start) {
    this.current = start;
  }

  @Override
  public Long peekNext() {
    return current;
  }

  @Override
  public Long takeNext() {
    var then = current;
    current = current + 1;
    return then;
  }

  @Override
  public List<Long> peekRange(final long size) {
    return LongStream.range(current, current + size).boxed().collect(Collectors.toList());
  }

  @Override
  public List<Long> takeRange(final long size) {
    var seq = peekRange(size);
    current += size;
    return seq;
  }
}

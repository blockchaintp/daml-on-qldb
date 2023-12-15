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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.function.Consumer;

import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;

/**
 *
 */
public final class ResultSetSpliterator implements Spliterator<ResultSet> {
  private ResultSet resultSet;
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(ResultSetSpliterator.class);

  /**
   *
   * @param theResultSet
   */
  public ResultSetSpliterator(final ResultSet theResultSet) {
    this.resultSet = theResultSet;
  }

  @Override
  public boolean tryAdvance(final Consumer<? super ResultSet> action) {
    try {
      if (this.resultSet.next()) {
        action.accept(this.resultSet);
        return true;
      }
    } catch (SQLException e) {
      LOG.error("Result set iterator error {}", e);
    }

    return false;
  }

  @Override
  public Spliterator<ResultSet> trySplit() {
    return null;
  }

  @Override
  public long estimateSize() {
    return 0;
  }

  @Override
  public int characteristics() {
    return 0;
  }
}

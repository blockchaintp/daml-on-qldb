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
import java.util.stream.StreamSupport;

import io.vavr.Tuple2;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import software.amazon.awssdk.services.qldb.model.QldbException;
import software.amazon.qldb.QldbDriver;

/**
 * Checks for QLDB and creates if needed, short circuiting after tables have been created.
 */
public final class RequiresTables {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(RequiresTables.class);
  private boolean allCreated;
  private final List<Tuple2<String, String>> tables;
  private final QldbDriver driver;

  /**
   *
   * @param tablesToCreate
   *          Tuples of (tablename,indexedfield).
   * @param theDriver
   *          A QLDB driver.
   */
  protected RequiresTables(final List<Tuple2<String, String>> tablesToCreate, final QldbDriver theDriver) {
    this.allCreated = false;
    this.tables = tablesToCreate;
    this.driver = theDriver;
  }

  private boolean tableExists(final String table) {
    return StreamSupport.stream(driver.getTableNames().spliterator(), false).map(t -> {
      LOG.debug("Looking for {} found table {}", table, t);
      return t;
    }).anyMatch(s -> s.equals(table));
  }

  /**
   *
   */
  public void checkTables() throws QldbException {
    if (allCreated) {
      return;
    }

    synchronized (tables) {
      if (allCreated) {
        return;
      }

      tables.forEach(t -> {
        if (tableExists(t._1)) {
          LOG.debug("Table {} exists, skip create", () -> t._1);
          return;
        }

        LOG.info("Creating table {}", () -> t._1);

        driver.execute(tx -> {
          tx.execute(String.format("create table %s", t._1));
          tx.execute(String.format("create index on %s(%s)", t._1, t._2));
        });
      });

      allCreated = true;
    }

  }

}

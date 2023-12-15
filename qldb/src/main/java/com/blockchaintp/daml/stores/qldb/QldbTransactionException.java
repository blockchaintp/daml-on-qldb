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
package com.blockchaintp.daml.stores.qldb;

import com.amazon.ion.IonValue;
import com.blockchaintp.daml.stores.exception.StoreException;

/**
 *
 */
public class QldbTransactionException extends StoreException {
  /**
   * A transaction log exception with an originating cause.
   *
   * @param cause
   *          the cause
   */
  protected QldbTransactionException(final Throwable cause) {
    super(cause);
  }

  /**
   * An exception with a message.
   *
   * @param message
   *          Pertinent message text.
   */
  protected QldbTransactionException(final String message) {
    super(message);
  }

  /**
   * We have retrieved a qldb record with an unexpected schema.
   *
   * @param value
   * @return The constructed exception
   */
  public static QldbTransactionException invalidSchema(final IonValue value) {
    return new QldbTransactionException(String.format("Invalid result from qldb %s", value.toPrettyString()));
  }

  /**
   * We have not managed to fetch metadata for a document.
   *
   * @param query
   *          The query that failed to fetch metadata
   * @return The constructed exception
   */
  public static QldbTransactionException noMetadata(final String query) {
    return new QldbTransactionException(String.format("Metadata query '%s' returned no results", query));
  }

  /**
   * An ion structure was exopected.
   *
   * @param value
   * @return An appropriate exception.
   */
  public static QldbTransactionException notAStruct(final IonValue value) {
    return new QldbTransactionException(String.format("Ion structure expected but got %s", value.toPrettyString()));
  }
}

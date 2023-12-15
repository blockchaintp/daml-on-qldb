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
 * The detail of a QLDBStore exception, note this is not used to wrap S3 service issues, but purely
 * for our own faults.
 */
public final class QldbStoreException extends StoreException {
  /**
   * An exception with a message.
   *
   * @param message
   *          Pertinent message text.
   */
  private QldbStoreException(final String message) {
    super(message);
  }

  /**
   * We have retrieved a qldb record with an unexpected schema.
   *
   * @param value
   * @return The constructed exception
   */
  public static QldbStoreException invalidSchema(final IonValue value) {
    return new QldbStoreException(String.format("Invalid result from qldb %s", value.toPrettyString()));
  }
}

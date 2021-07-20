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
package com.blockchaintp.daml.stores.exception;

/**
 * Represents a write failure of some sort to the store.
 */
public class StoreWriteException extends StoreException {

  /**
   * Write exception with cause and error message.
   *
   * @param error
   *          the error message
   * @param cause
   *          the originating exception
   */
  public StoreWriteException(final String error, final Throwable cause) {
    super(error, cause);
  }

  /**
   * A write exception with an originating cause.
   *
   * @param cause
   *          the cause
   */
  public StoreWriteException(final Throwable cause) {
    super(cause);
  }
}

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
 * Represents a read failure of some sort to the store.
 */
public class StoreReadException extends StoreException {

  /**
   * Read exception with cause and error message.
   *
   * @param error
   *          the error message
   * @param cause
   *          the originating exception
   */
  public StoreReadException(final String error, final Throwable cause) {
    super(error, cause);
  }

  /**
   * A read exception with an originating cause.
   *
   * @param cause
   *          the cause
   */
  public StoreReadException(final Throwable cause) {
    super(cause);
  }

}

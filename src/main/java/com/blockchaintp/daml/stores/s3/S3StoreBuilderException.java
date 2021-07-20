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
package com.blockchaintp.daml.stores.s3;

/**
 * RuntimeExceptions relating to the construction of S3Store objects.
 */
public class S3StoreBuilderException extends RuntimeException {
  /**
   * Exception with message and cause.
   *
   * @param message
   *          the message
   * @param cause
   *          the originating exception
   */
  public S3StoreBuilderException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Exception with message.
   *
   * @param message
   *          the message
   */
  public S3StoreBuilderException(final String message) {
    super(message);
  }
}

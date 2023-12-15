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
package com.blockchaintp.exception;

import java.security.NoSuchAlgorithmException;

/**
 * A serious error that indicates that the JVM environment does not support SHA-512.
 */
public class NoSHA512SupportException extends RuntimeException {

  /**
   * Exception with cause.
   *
   * @param cause
   */
  public NoSHA512SupportException(final NoSuchAlgorithmException cause) {
    super("No SHA-512 support available", cause);
  }
}

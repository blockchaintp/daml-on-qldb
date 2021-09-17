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
package com.blockchaintp.daml.stores.postgres;

import com.blockchaintp.daml.stores.exception.StoreException;

/**
 * Non sql exceptions for the postgres txlog.
 */
public final class PostgresTxLogException extends StoreException {
  private PostgresTxLogException(final String msg) {
    super(msg);
  }

  /**
   *
   * @return The appropriate exception.
   */
  public static PostgresTxLogException noInsertResult() {
    return new PostgresTxLogException("No result from insert");
  }
}

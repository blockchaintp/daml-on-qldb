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
package com.blockchaintp.daml.stores.layers;

import com.blockchaintp.daml.stores.exception.StoreReadException;

/**
 * An exception on a split store operation.
 */
public final class SpltStoreException extends StoreReadException {

  private SpltStoreException(final String message) {
    super(message);
  }

  /**
   * The corresponding s3 blob for a transaction could not be found.
   *
   * @param hash
   * @param key
   * @return The exception.
   */
  public static SpltStoreException missingS3Data(final String hash, final String key) {
    return new SpltStoreException(String.format("Cannot find s3 blob %s for key %s", hash, key));
  }
}

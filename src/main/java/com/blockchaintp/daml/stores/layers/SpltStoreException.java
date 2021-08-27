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

import java.util.List;
import java.util.stream.Collectors;

import com.blockchaintp.daml.stores.exception.StoreException;

import io.vavr.Tuple2;

/**
 * An exception on a split store operation.
 */
public final class SpltStoreException extends StoreException {

  private SpltStoreException(final String message) {
    super(message);
  }

  /**
   * The corresponding s3 blobs for a transaction could not be found.
   *
   * @param missing
   * @return The exception.
   */
  public static SpltStoreException missingData(final List<Tuple2<String, String>> missing) {
    return new SpltStoreException(String.format("Cannot find s3 data (%s)",
        missing.stream().map(x -> String.format("%s:%s", x._1, x._2)).collect(Collectors.joining())));
  }
}

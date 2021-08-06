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

/**
 * Configuration for a{@link RetryingStore} layer.
 */
public class RetryingConfig {
  private static final int DEFAULT_MAX_RETRIES = 3;
  /**
   * The maximum number of retries.
   */
  private int maxRetries = DEFAULT_MAX_RETRIES;

  /**
   * @return the maxRetries
   */
  public int getMaxRetries() {
    return maxRetries;
  }

  /**
   * @param retries
   *          the maxRetries to set
   */
  public void setMaxRetries(final int retries) {
    this.maxRetries = retries;
  }
}
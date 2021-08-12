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
package com.blockchaintp.utility;

import java.util.Locale;

/**
 * Aws utility functions.
 */
public final class Aws {
  private static final int S3_LENGTH = 63;
  private static final int QLDB_LENGTH = 31;

  /**
   * Force lower case, replace underscores with hyphens, truncate.
   *
   * @param name
   * @return A valid name.
   */
  public static String complyWithQldbLedgerNaming(final String name) {
    var filtered = name.toLowerCase(Locale.ROOT).replace("_", "-");
    return filtered.substring(0, Math.min(filtered.length(), QLDB_LENGTH));
  }

  /**
   * S£ bucket name rules are rather complicated, this is just a character filter and underscore to
   * hyphen converter.
   *
   * @param name
   * @return A valid name
   */
  public static String complyWithS3BucketNaming(final String name) {
    var filtered = name.toLowerCase(Locale.ROOT).replace("_", "-");
    return filtered.substring(0, Math.min(filtered.length(), S3_LENGTH));
  }

  private Aws() {
  }
}

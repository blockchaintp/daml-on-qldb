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

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Converting UUIDs to ana from byte arrays.
 */
public final class UuidConverter {
  private static final int UUID_LENGTH_IN_BYTES = 16;

  private UuidConverter() {
  }

  /**
   *
   * @param bytes
   * @return The UUID represented by the supplied bytes.
   */
  public static UUID asUuid(final byte[] bytes) {
    var bb = ByteBuffer.wrap(bytes);
    var firstLong = bb.getLong();
    var secondLong = bb.getLong();
    return new UUID(firstLong, secondLong);
  }

  /**
   *
   * @param uuid
   * @return The bytewise representation of the UUID.
   */
  public static byte[] asBytes(final UUID uuid) {
    var bb = ByteBuffer.wrap(new byte[UUID_LENGTH_IN_BYTES]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }
}

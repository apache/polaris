/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.extension.persistence.relational.jdbc;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.UUID;

public class IdGenerator {
  private IdGenerator() {}

  public static final IdGenerator idGenerator = new IdGenerator();

  private static final long LONG_MAX_ID = 0x7fffffffffffffffL;

  public long nextId() {
    // Make sure this is a positive number.
    // conflicting ids don't get accepted and is enforced by table constraints.
    return generateSecureRandomUUID().getLeastSignificantBits() & LONG_MAX_ID;
  }

  private UUID generateSecureRandomUUID() {
    SecureRandom secureRandom = new SecureRandom();
    byte[] randomBytes = new byte[16];
    secureRandom.nextBytes(randomBytes);

    // Ensure the most significant bits of the time_hi_and_version field are 0001
    randomBytes[6] &= 0x0f; // Clear version bits
    randomBytes[6] |= 0x40; // Set version to 4 (random)

    // Ensure the most significant bits of the clock_seq_hi_and_reserved field are 10
    randomBytes[8] &= 0x3f; // Clear variant bits
    randomBytes[8] |= 0x80; // Set variant to RFC 4122

    ByteBuffer bb = ByteBuffer.wrap(randomBytes);
    long mostSigBits = bb.getLong();
    long leastSigBits = bb.getLong();

    return new UUID(mostSigBits, leastSigBits);
  }
}

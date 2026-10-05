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
package org.apache.polaris.persistence.atomic.dynamodb;

import java.security.SecureRandom;

/**
 * Positive 63-bit random id generator, mirroring {@code relational-jdbc}'s {@code IdGenerator}.
 *
 * <p>A collision would be rejected by the create path's {@code attribute_not_exists(PK)} condition
 * (the entity item's partition key is {@code realmId#id}), so an id clash surfaces as a retryable
 * create failure rather than silent overwrite — the same backstop the JDBC backend relies on via
 * its primary-key constraint.
 */
final class IdGenerator {

  private static final IdGenerator INSTANCE = new IdGenerator();
  private static final long LONG_MAX_ID = 0x7fffffffffffffffL;

  private final SecureRandom secureRandom = new SecureRandom();

  private IdGenerator() {}

  static IdGenerator getIdGenerator() {
    return INSTANCE;
  }

  long nextId() {
    // Mask the sign bit so the id is always positive.
    return secureRandom.nextLong() & LONG_MAX_ID;
  }
}

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
package org.apache.polaris.persistence.relational.jdbc;

import java.security.SecureRandom;

public class IdGenerator {
  private static final IdGenerator idGenerator = new IdGenerator();

  public static IdGenerator getIdGenerator() {
    return idGenerator;
  }

  private final SecureRandom secureRandom = new SecureRandom();
  private static final long LONG_MAX_ID = 0x7fffffffffffffffL;

  private IdGenerator() {}

  public long nextId() {
    // Make sure this is a positive number.
    // conflicting ids don't get accepted and is enforced by table constraints.
    return secureRandom.nextLong() & LONG_MAX_ID;
  }
}

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
package org.apache.polaris.ids.api;

import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.util.UUID;

public interface SnowflakeIdGenerator extends IdGenerator {
  /** Offset of the snowflake ID generator since the 1970-01-01T00:00:00Z epoch instant. */
  Instant ID_EPOCH = Instant.parse("2025-03-01T00:00:00Z");

  /**
   * Offset of the snowflake ID generator in milliseconds since the 1970-01-01T00:00:00Z epoch
   * instant.
   */
  long ID_EPOCH_MILLIS = ID_EPOCH.toEpochMilli();

  int DEFAULT_NODE_ID_BITS = 10;
  int DEFAULT_TIMESTAMP_BITS = 41;
  int DEFAULT_SEQUENCE_BITS = 12;

  long constructId(long timestamp, long sequence, long node);

  long timestampFromId(long id);

  long timestampUtcFromId(long id);

  long sequenceFromId(long id);

  long nodeFromId(long id);

  UUID idToTimeUuid(long id);

  String idToString(long id);

  long timeUuidToId(@Nonnull UUID uuid);

  int timestampBits();

  int sequenceBits();

  int nodeIdBits();
}

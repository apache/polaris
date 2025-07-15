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
package org.apache.polaris.ids.impl;

import static org.apache.polaris.ids.impl.SnowflakeIdGeneratorImpl.validateArguments;

import java.time.Instant;
import java.util.Map;
import org.apache.polaris.ids.api.SnowflakeIdGenerator;
import org.apache.polaris.ids.spi.IdGeneratorFactory;
import org.apache.polaris.ids.spi.IdGeneratorSource;

public class SnowflakeIdGeneratorFactory implements IdGeneratorFactory<SnowflakeIdGenerator> {
  @Override
  public void validateParameters(Map<String, String> params, IdGeneratorSource idGeneratorSource) {
    int timestampBits =
        Integer.parseInt(
            params.getOrDefault(
                "timestamp-bits", "" + SnowflakeIdGenerator.DEFAULT_TIMESTAMP_BITS));
    int nodeIdBits =
        Integer.parseInt(
            params.getOrDefault("node-id-bits", "" + SnowflakeIdGenerator.DEFAULT_NODE_ID_BITS));
    int sequenceBits =
        Integer.parseInt(
            params.getOrDefault("sequence-bits", "" + SnowflakeIdGenerator.DEFAULT_SEQUENCE_BITS));
    var idEpochMillis = SnowflakeIdGenerator.ID_EPOCH_MILLIS;
    var idEpochMillisFromParams = params.get("offset");
    if (idEpochMillisFromParams != null) {
      idEpochMillis = Instant.parse(idEpochMillisFromParams).toEpochMilli();
    }

    validateArguments(timestampBits, sequenceBits, nodeIdBits, idEpochMillis, idGeneratorSource);
  }

  @Override
  public SnowflakeIdGenerator buildSystemIdGenerator(Map<String, String> params) {
    return buildIdGenerator(
        params,
        new IdGeneratorSource() {
          @Override
          public int nodeId() {
            return 0;
          }

          @Override
          public long currentTimeMillis() {
            return SnowflakeIdGenerator.ID_EPOCH_MILLIS;
          }
        });
  }

  @Override
  public SnowflakeIdGenerator buildIdGenerator(
      Map<String, String> params, IdGeneratorSource idGeneratorSource) {
    int timestampBits =
        Integer.parseInt(
            params.getOrDefault(
                "timestamp-bits", "" + SnowflakeIdGenerator.DEFAULT_TIMESTAMP_BITS));
    int nodeIdBits =
        Integer.parseInt(
            params.getOrDefault("node-id-bits", "" + SnowflakeIdGenerator.DEFAULT_NODE_ID_BITS));
    int sequenceBits =
        Integer.parseInt(
            params.getOrDefault("sequence-bits", "" + SnowflakeIdGenerator.DEFAULT_SEQUENCE_BITS));
    // ATCFIX - This name is incorrect.
    var idEpochMillis = SnowflakeIdGenerator.ID_EPOCH_MILLIS;
    var offset = params.get("offset");
    if (offset != null) {
      idEpochMillis = Instant.parse(offset).toEpochMilli();
    }

    return buildIdGenerator(
        timestampBits, sequenceBits, nodeIdBits, idEpochMillis, idGeneratorSource);
  }

  public SnowflakeIdGenerator buildIdGenerator(
      int timestampBits,
      int sequenceBits,
      int nodeIdBits,
      long offsetMillis,
      IdGeneratorSource idGeneratorSource) {
    return new SnowflakeIdGeneratorImpl(
        timestampBits, sequenceBits, nodeIdBits, offsetMillis, idGeneratorSource);
  }

  @Override
  public String name() {
    return "snowflake";
  }
}

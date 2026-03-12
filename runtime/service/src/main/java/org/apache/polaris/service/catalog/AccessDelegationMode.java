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
package org.apache.polaris.service.catalog;

import com.google.common.base.Functions;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents access mechanisms defined in the Iceberg REST API specification (values for the {@code
 * X-Iceberg-Access-Delegation} header).
 */
public enum AccessDelegationMode {
  VENDED_CREDENTIALS("vended-credentials"),
  REMOTE_SIGNING("remote-signing"),
  ;

  AccessDelegationMode(String protocolValue) {
    this.protocolValue = protocolValue;
  }

  private final String protocolValue;

  public String protocolValue() {
    return protocolValue;
  }

  public static EnumSet<AccessDelegationMode> fromProtocolValuesList(String protocolValues) {
    if (protocolValues == null || protocolValues.isEmpty()) {
      return EnumSet.noneOf(AccessDelegationMode.class);
    }

    // Backward-compatibility case for old clients that still use the unofficial value of `true` to
    // request credential vending. Note that if the client requests `true` among other values it
    // will be parsed as `UNKNOWN` (by the code below this `if`) since the client submitting
    // multiple access modes is expected to be aware of the Iceberg REST API spec.
    if (protocolValues.trim().toLowerCase(Locale.ROOT).equals("true")) {
      return EnumSet.of(VENDED_CREDENTIALS);
    }

    EnumSet<AccessDelegationMode> set = EnumSet.noneOf(AccessDelegationMode.class);
    Arrays.stream(protocolValues.split(",")) // per Iceberg REST Catalog spec
        .map(String::trim)
        .map(Mapper::map)
        .filter(Objects::nonNull)
        .forEach(set::add);
    return set;
  }

  private static class Mapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(Mapper.class);

    private static final Map<String, AccessDelegationMode> BY_PROTOCOL_VALUE =
        Arrays.stream(AccessDelegationMode.values())
            .collect(Collectors.toMap(AccessDelegationMode::protocolValue, Functions.identity()));

    @Nullable
    public static AccessDelegationMode map(String key) {
      AccessDelegationMode mode = Mapper.BY_PROTOCOL_VALUE.get(key);
      if (mode == null) {
        LOGGER.warn("Unknown access delegation mode requested: {}", key);
      }
      return mode;
    }
  }
}

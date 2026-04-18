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
package org.apache.polaris.service.http;

import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.Optional;

/** Parses Iceberg client HTTP request headers into an {@link IcebergClientInfo}. */
@ApplicationScoped
public class IcebergClientInfoFactory {

  /** The HTTP header name carrying the client version string. */
  static final String CLIENT_VERSION_HEADER = "X-Client-Version";

  /** Creates an {@link IcebergClientInfo} by parsing the Iceberg client headers. */
  @Produces
  @RequestScoped
  public IcebergClientInfo create(HttpHeaders httpHeaders) {
    return create(
        httpHeaders.getHeaderString(CLIENT_VERSION_HEADER),
        httpHeaders.getHeaderString(HttpHeaders.USER_AGENT));
  }

  /** Creates an {@link IcebergClientInfo} from raw header values; exposed for testing. */
  public IcebergClientInfo create(
      @Nullable String versionHeader, @Nullable String userAgentHeader) {

    String icebergVersion = null;
    IcebergClientInfo.Language language = null;

    // 1. Parse X-Client-Version for Java clients
    if (versionHeader != null && !versionHeader.isEmpty()) {

      if (versionHeader.startsWith("Apache Iceberg")) {
        // Java: "Apache Iceberg <version> (commit <fullHash>)"
        int start = "Apache Iceberg ".length();
        int end = versionHeader.indexOf(' ', start);
        if (end > start
            && versionHeader.regionMatches(end, " (commit ", 0, " (commit ".length())
            && versionHeader.endsWith(")")) {
          icebergVersion = versionHeader.substring(start, end);
          language = IcebergClientInfo.Language.JAVA;
        }
      }
    }

    // 2. Parse User-Agent for other clients, which (except for Python) send a stale, hardcoded
    // constant in X-Client-Version, or do not send it at all.
    if (icebergVersion == null && userAgentHeader != null && !userAgentHeader.isEmpty()) {

      if (userAgentHeader.startsWith("PyIceberg/")) {
        // Python: "PyIceberg/<version>"
        int start = "PyIceberg/".length();
        icebergVersion = userAgentHeader.substring(start);
        language = IcebergClientInfo.Language.PYTHON;

      } else if (userAgentHeader.startsWith("GoIceberg/")) {
        // Go: "GoIceberg/<version>"
        int start = "GoIceberg/".length();
        icebergVersion = userAgentHeader.substring(start);
        language = IcebergClientInfo.Language.GO;

      } else if (userAgentHeader.startsWith("iceberg-rs/")) {
        // Rust: "iceberg-rs/<version>"
        int start = "iceberg-rs/".length();
        icebergVersion = userAgentHeader.substring(start);
        language = IcebergClientInfo.Language.RUST;

      } else if (userAgentHeader.startsWith("iceberg-cpp/")) {
        // C++: "iceberg-cpp/<version>"
        int start = "iceberg-cpp/".length();
        icebergVersion = userAgentHeader.substring(start);
        language = IcebergClientInfo.Language.CPP;
      }
    }

    return ImmutableIcebergClientInfo.of(
        Optional.ofNullable(icebergVersion), Optional.ofNullable(language));
  }
}

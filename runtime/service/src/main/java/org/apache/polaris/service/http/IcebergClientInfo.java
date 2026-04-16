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

import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * Holds parsed information from the Iceberg client HTTP request headers.
 *
 * <p>Iceberg REST clients send a standard {@code X-Client-Version} header on every request: a
 * client-identification string whose format and semantics vary by language implementation (see
 * below). In addition, all HTTP clients send a standard {@code User-Agent} header, which some
 * Iceberg client implementations use to carry their library version.
 *
 * <p>The following table summarizes the known formats and the source of {@link #icebergVersion()}
 * for each language:
 *
 * <table border="1">
 *   <tr><th>Language</th><th>{@code X-Client-Version}</th>
 *       <th>User-Agent</th><th>{@link #icebergVersion()} source</th></tr>
 *   <tr><td><a href="https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/rest/HTTPClient.java">Java</a></td>
 *       <td>{@code Apache Iceberg <lib version> (commit <hash>)}</td>
 *       <td>{@code Apache-HttpClient/5.x (Java/...)}</td>
 *       <td>{@code X-Client-Version}</td></tr>
 *   <tr><td><a href="https://github.com/apache/iceberg-python/blob/main/pyiceberg/catalog/rest/__init__.py">Python (PyIceberg)</a></td>
 *       <td>{@code PyIceberg <lib version>}</td>
 *       <td>{@code PyIceberg/<lib version>}</td>
 *       <td>{@code User-Agent}</td></tr>
 *   <tr><td><a href="https://github.com/apache/iceberg-go/blob/main/catalog/rest/rest.go">Go (iceberg-go)</a></td>
 *       <td>a stale hardcoded constant, not the client version</td>
 *       <td>{@code GoIceberg/<lib version>}</td>
 *       <td>{@code User-Agent}</td></tr>
 *   <tr><td><a href="https://github.com/apache/iceberg-rust/blob/main/crates/catalog/rest/src/catalog.rs">Rust (iceberg-rust)</a></td>
 *       <td>a stale hardcoded constant, not the client version</td>
 *       <td>{@code iceberg-rs/<lib version>}</td>
 *       <td>{@code User-Agent}</td></tr>
 *   <tr><td><a href="https://github.com/apache/iceberg-cpp/blob/main/src/iceberg/catalog/rest/http_client.cc">C++ (iceberg-cpp)</a></td>
 *       <td>not sent</td>
 *       <td>{@code iceberg-cpp/<lib version>}</td>
 *       <td>{@code User-Agent}</td></tr>
 * </table>
 */
@PolarisImmutable
public interface IcebergClientInfo {

  /** Programming language of an Iceberg REST client, as determined from request headers. */
  enum Language {
    JAVA,
    PYTHON,
    GO,
    RUST,
    CPP
  }

  /** Returns the Iceberg client library version, or empty if it cannot be determined. */
  @Value.Parameter(order = 1)
  Optional<String> icebergVersion();

  /** Returns the client language, or empty if it cannot be determined. */
  @Value.Parameter(order = 2)
  Optional<Language> language();
}

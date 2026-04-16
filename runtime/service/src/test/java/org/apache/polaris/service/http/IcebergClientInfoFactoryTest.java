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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class IcebergClientInfoFactoryTest {

  // --- Java client (org.apache.iceberg.rest.HTTPClient) ---
  // X-Client-Version: "Apache Iceberg <version> (commit <fullHash>)"
  // User-Agent:       "Apache-HttpClient/5.x (Java/11.x)" <-- HTTP library, not Iceberg

  @Test
  void javaClient() {
    IcebergClientInfo info =
        new IcebergClientInfoFactory()
            .create(
                "Apache Iceberg 1.10.1 (commit abc1234567890abcdef1234567890abcdef123456)",
                "Apache-HttpClient/5.4 (Java/11.0.25)");
    assertThat(info.icebergVersion()).hasValue("1.10.1");
    assertThat(info.language()).hasValue(IcebergClientInfo.Language.JAVA);
  }

  @Test
  void javaClientSnapshotVersion() {
    IcebergClientInfo info =
        new IcebergClientInfoFactory()
            .create(
                "Apache Iceberg 1.11.0-SNAPSHOT (commit abc1234567890abcdef1234567890abcdef123456)",
                null);
    assertThat(info.icebergVersion()).hasValue("1.11.0-SNAPSHOT");
    assertThat(info.language()).hasValue(IcebergClientInfo.Language.JAVA);
  }

  @Test
  void javaClientUnknownBuildMetadata() {
    // IcebergBuild returns "unknown" for both fields when build properties cannot be loaded
    IcebergClientInfo info =
        new IcebergClientInfoFactory().create("Apache Iceberg unknown (commit unknown)", null);
    assertThat(info.icebergVersion()).hasValue("unknown");
    assertThat(info.language()).hasValue(IcebergClientInfo.Language.JAVA);
  }

  // --- Python client (PyIceberg) ---
  // X-Client-Version: "PyIceberg <version>"   (pyiceberg/catalog/rest/__init__.py)
  // User-Agent:       "PyIceberg/<version>"

  @Test
  void pythonClient() {
    IcebergClientInfo info =
        new IcebergClientInfoFactory().create("PyIceberg 0.9.1", "PyIceberg/0.9.1");
    assertThat(info.icebergVersion()).hasValue("0.9.1");
    assertThat(info.language()).hasValue(IcebergClientInfo.Language.PYTHON);
  }

  // --- Go client (iceberg-go) ---
  // X-Client-Version: a stale hardcoded constant — ignored  (catalog/rest/rest.go)
  // User-Agent:       "GoIceberg/<libVersion>"

  @Test
  void goClient() {
    IcebergClientInfo info = new IcebergClientInfoFactory().create("0.14.1", "GoIceberg/0.5.0");
    assertThat(info.icebergVersion()).hasValue("0.5.0");
    assertThat(info.language()).hasValue(IcebergClientInfo.Language.GO);
  }

  // --- Rust client (iceberg-rust) ---
  // X-Client-Version: a stale hardcoded constant — ignored  (crates/catalog/rest/src/catalog.rs)
  // User-Agent:       "iceberg-rs/<libVersion>"

  @Test
  void rustClient() {
    IcebergClientInfo info = new IcebergClientInfoFactory().create("0.14.1", "iceberg-rs/0.3.0");
    assertThat(info.icebergVersion()).hasValue("0.3.0");
    assertThat(info.language()).hasValue(IcebergClientInfo.Language.RUST);
  }

  // --- C++ client (iceberg-cpp) ---
  // X-Client-Version: not sent  (src/iceberg/catalog/rest/http_client.cc)
  // User-Agent:       "iceberg-cpp/<libVersion>"

  @Test
  void cppClient() {
    IcebergClientInfo info =
        new IcebergClientInfoFactory().create(null, "iceberg-cpp/0.3.0-SNAPSHOT");
    assertThat(info.icebergVersion()).hasValue("0.3.0-SNAPSHOT");
    assertThat(info.language()).hasValue(IcebergClientInfo.Language.CPP);
  }

  // --- Missing / unrecognized headers ---

  @Test
  void noHeaders() {
    IcebergClientInfo info = new IcebergClientInfoFactory().create(null, null);
    assertThat(info.icebergVersion()).isEmpty();
    assertThat(info.language()).isEmpty();
    info = new IcebergClientInfoFactory().create("", "");
    assertThat(info.icebergVersion()).isEmpty();
    assertThat(info.language()).isEmpty();
  }

  @Test
  void unknownHeaders() {
    IcebergClientInfo info =
        new IcebergClientInfoFactory().create("some-unknown-format", "some-unknown-agent");
    assertThat(info.icebergVersion()).isEmpty();
    assertThat(info.language()).isEmpty();
  }
}

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
package org.apache.polaris.core.storage.gcp;

import java.nio.file.Path;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * Pre-computed GCS principal attribution configuration parameters, evaluated at cache key build
 * time. Present in the cache key only when attribution is fully configured and a principal is
 * available; absent otherwise.
 */
@PolarisImmutable
public interface GcpAttributionParams {

  @Value.Parameter(order = 1)
  String tokenIssuer();

  @Value.Parameter(order = 2)
  String wifAudience();

  @Value.Parameter(order = 3)
  String signingKeyFile();

  @Value.Parameter(order = 4)
  String signingKeyId();

  /** Path derived from {@link #signingKeyFile()}; validated at construction time. */
  @Value.Derived
  default Path signingKeyPath() {
    return Path.of(signingKeyFile());
  }

  static GcpAttributionParams of(
      String tokenIssuer, String wifAudience, String signingKeyFile, String signingKeyId) {
    return ImmutableGcpAttributionParams.of(tokenIssuer, wifAudience, signingKeyFile, signingKeyId);
  }
}

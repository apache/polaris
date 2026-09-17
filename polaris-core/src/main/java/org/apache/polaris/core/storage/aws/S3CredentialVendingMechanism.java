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

package org.apache.polaris.core.storage.aws;

import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.jspecify.annotations.Nullable;

/**
 * How Polaris vends S3 credentials for one S3 catalog. Implementations are CDI beans annotated with
 * {@code @Identifier("<mechanism>")}; a catalog's {@code credentialVendingMechanism} selects one by
 * that identifier, and a catalog that leaves the field empty selects {@link #DEFAULT}. A realm
 * lists the explicit mechanisms it accepts in {@code SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS}; a
 * listed mechanism with no bean in the running server is refused wherever a catalog selects it. A
 * server replaces a mechanism, {@link #DEFAULT} included, with an {@code @Alternative} bean of a
 * higher {@code @Priority} that carries the same identifier. An implementation must be
 * application-scoped (or otherwise normal-scoped): the registry resolves every bean once at startup
 * and hands out the same instance for the lifetime of the server. An implementation that needs
 * realm configuration injects {@code RealmConfig}; the request context is active wherever the
 * server calls a mechanism, including task execution.
 */
public interface S3CredentialVendingMechanism {

  /** AWS STS AssumeRole against the catalog's role. */
  String STS = "STS";

  /**
   * The server's default mechanism, selected by leaving {@code credentialVendingMechanism} empty.
   * The identifier itself is reserved: a request that names it is refused. Polaris maps it to
   * {@link #STS}; a server build may install a different default.
   */
  String DEFAULT = "DEFAULT";

  /** The storage integration that vends for one S3 catalog under this mechanism. */
  PolarisStorageIntegration integrationFor(AwsStorageConfigurationInfo storageConfig);

  /**
   * Checks a storage config that selects this mechanism before it is stored. Called at catalog
   * create with {@code current} null, and at catalog update with the stored config as {@code
   * current}, after authorization, the realm allowlist and the availability check. Throw {@link
   * IllegalArgumentException} to refuse the request with HTTP 400. The default accepts every
   * config.
   */
  default void validate(
      @Nullable AwsStorageConfigurationInfo current, AwsStorageConfigurationInfo updated) {}
}

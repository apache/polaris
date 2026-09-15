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

import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.PolarisStorageIntegration;

/**
 * How Polaris vends S3 credentials for one S3 catalog. Implementations are CDI beans annotated with
 * {@code @Identifier("<mechanism>")}; a catalog's {@code credentialVendingMechanism} selects one by
 * that identifier. A realm lists the mechanisms it accepts in {@code
 * SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS}; a listed mechanism with no bean in the running
 * server is refused wherever the catalog is opened or a credential is needed.
 */
public interface S3CredentialVendingMechanism {

  /** AWS STS AssumeRole against the catalog's role, the default. */
  String STS = "STS";

  /** Cloudflare R2 temporary credentials signed by the server with a parent token. */
  String CLOUDFLARE_R2 = "CLOUDFLARE_R2";

  /** The storage integration that vends for one S3 catalog under this mechanism. */
  PolarisStorageIntegration integrationFor(
      AwsStorageConfigurationInfo storageConfig, RealmConfig realmConfig);
}

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
package org.apache.polaris.service.storage;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.apache.polaris.core.storage.aws.StsClientProvider;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * The server's default mechanism, selected by an empty {@code credentialVendingMechanism}: AWS STS
 * AssumeRole.
 */
@ApplicationScoped
@Identifier(S3CredentialVendingMechanism.DEFAULT)
public class DefaultCredentialVendingMechanism extends AbstractStsCredentialVendingMechanism {

  @Inject
  public DefaultCredentialVendingMechanism(
      StorageConfiguration storageConfiguration,
      StsClientProvider stsClientProvider,
      StorageCredentialCache cache,
      RealmConfig realmConfig) {
    super(storageConfiguration, stsClientProvider, cache, realmConfig);
  }

  /** Test constructor: a fixed credentials provider and the realm config to vend under. */
  public DefaultCredentialVendingMechanism(
      StsClientProvider stsClientProvider,
      Optional<AwsCredentialsProvider> stsCredentials,
      StorageCredentialCache cache,
      RealmConfig realmConfig) {
    super(stsClientProvider, stsCredentials, cache, realmConfig);
  }
}

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

package org.apache.polaris.service.identity.provider;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.credential.AwsIamServiceIdentityCredential;
import org.apache.polaris.core.identity.credential.ServiceIdentityCredential;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * Application-scoped cache of resolved {@link ServiceIdentityCredential}s, keyed by the realm whose
 * configuration they were resolved from and their identity type.
 *
 * <p>{@link DefaultServiceIdentityProvider} is request scoped, so without this cache every request
 * would build a new credential and, with it, a new AWS credentials provider. With the AWS default
 * credential chain (e.g. web identity on EKS), each provider instance owns its own STS client and
 * credential cache, so a new instance per request means a new STS call per request. Service
 * identity configuration is fixed at startup, so a credential resolved for a reference stays valid
 * for the lifetime of the application, and the SDK provider refreshes the underlying session
 * credentials itself.
 */
@ApplicationScoped
public class ServiceIdentityCredentialCache {

  private record Key(String realm, ServiceIdentityType type) {}

  private final ConcurrentMap<Key, ServiceIdentityCredential> credentials =
      new ConcurrentHashMap<>();

  /**
   * Returns the credential cached for the given realm and identity type, resolving it with {@code
   * resolver} on first use. Empty results are not cached.
   */
  Optional<ServiceIdentityCredential> getOrResolve(
      String realm,
      ServiceIdentityType type,
      Supplier<Optional<? extends ServiceIdentityCredential>> resolver) {
    return Optional.ofNullable(
        credentials.computeIfAbsent(new Key(realm, type), key -> resolver.get().orElse(null)));
  }

  @PreDestroy
  void shutdown() {
    credentials
        .values()
        .forEach(
            credential -> {
              if (credential instanceof AwsIamServiceIdentityCredential awsCredential
                  && awsCredential.getAwsCredentialsProvider()
                      instanceof SdkAutoCloseable closeable) {
                closeable.close();
              }
            });
    credentials.clear();
  }
}

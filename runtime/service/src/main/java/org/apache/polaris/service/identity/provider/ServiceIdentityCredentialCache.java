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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * Application-scoped cache of the AWS credentials providers behind service identity credentials,
 * keyed by the realm whose configuration they were created from.
 *
 * <p>{@link DefaultServiceIdentityProvider} is request scoped, so without this cache every request
 * would build a new AWS credentials provider. With the AWS default credential chain (e.g. web
 * identity on EKS), each provider instance owns its own STS client and credential cache, so a new
 * instance per request means a new STS call per request. Service identity configuration is fixed at
 * startup, so a provider created for a realm stays valid for the lifetime of the application, and
 * it refreshes the underlying session credentials itself.
 */
@ApplicationScoped
public class ServiceIdentityCredentialCache {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ServiceIdentityCredentialCache.class);

  private final ConcurrentMap<String, AwsCredentialsProvider> awsCredentialsProviders =
      new ConcurrentHashMap<>();

  /**
   * Returns the AWS credentials provider cached for {@code realm}, creating it with {@code factory}
   * on first use.
   */
  AwsCredentialsProvider awsCredentialsProvider(
      String realm, Supplier<AwsCredentialsProvider> factory) {
    return awsCredentialsProviders.computeIfAbsent(realm, key -> factory.get());
  }

  @PreDestroy
  void shutdown() {
    awsCredentialsProviders.forEach(
        (realm, provider) -> {
          if (provider instanceof SdkAutoCloseable closeable) {
            try {
              closeable.close();
            } catch (RuntimeException e) {
              LOGGER.warn("Failed to close AWS credentials provider for realm {}", realm, e);
            }
          }
        });
    awsCredentialsProviders.clear();
  }
}

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

package org.apache.polaris.service.identity;

import io.smallrye.config.WithName;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Represents service identity configuration for a specific realm.
 *
 * <p>Supports multiple identity types, such as AWS IAM. This interface allows each realm to define
 * the credentials and metadata needed to resolve service-managed identities.
 */
public interface RealmServiceIdentityConfiguration {
  /**
   * Returns the AWS IAM service identity configuration for this realm, if present.
   *
   * @return an optional AWS IAM configuration
   */
  @WithName("aws-iam")
  Optional<AwsIamServiceIdentityConfiguration> awsIamServiceIdentity();

  /**
   * Aggregates all configured service identity types into a list. This includes AWS IAM and
   * potentially other types in the future.
   *
   * @return a list of configured service identity definitions
   */
  default List<? extends ResolvableServiceIdentityConfiguration> serviceIdentityConfigurations() {
    return Stream.of(awsIamServiceIdentity()).flatMap(Optional::stream).toList();
  }

  /**
   * Returns the default realm service identity configuration.
   *
   * <p>This configuration is used only when the default realm ({@code DEFAULT_REALM_KEY}) has no
   * explicit configuration. It serves as a fallback for development scenarios where credentials are
   * obtained from the environment without requiring explicit configuration.
   *
   * @return the default realm service identity configuration
   */
  static RealmServiceIdentityConfiguration defaultConfiguration() {
    return new RealmServiceIdentityConfiguration() {
      @Override
      public Optional<AwsIamServiceIdentityConfiguration> awsIamServiceIdentity() {
        // Return the AWS-specific default configuration that uses environment credentials
        return Optional.of(AwsIamServiceIdentityConfiguration.defaultConfiguration());
      }
    };
  }
}

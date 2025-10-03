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

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.credential.ServiceIdentityCredential;
import org.apache.polaris.core.identity.dpo.AwsIamServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.secrets.SecretReference;
import org.apache.polaris.service.identity.RealmServiceIdentityConfiguration;
import org.apache.polaris.service.identity.ResolvableServiceIdentityConfiguration;
import org.apache.polaris.service.identity.ServiceIdentityConfiguration;

/**
 * Default implementation of {@link ServiceIdentityProvider} that provides service identity
 * credentials from statically configured values.
 *
 * <p>This implementation loads service identity configurations at startup and uses them to provide
 * identity information and credentials on demand. All resolution is done lazily - credentials are
 * only created when actually needed for authentication.
 */
@RequestScoped
public class DefaultServiceIdentityProvider implements ServiceIdentityProvider {
  public static final String DEFAULT_REALM_KEY = ServiceIdentityConfiguration.DEFAULT_REALM_KEY;
  public static final String DEFAULT_REALM_NSS = "system:default";
  private static final String IDENTITY_INFO_REFERENCE_URN_FORMAT =
      "urn:polaris-secret:default-identity-provider:%s:%s";

  private final String realm;
  private final RealmServiceIdentityConfiguration config;

  public DefaultServiceIdentityProvider() {
    this.realm = DEFAULT_REALM_KEY;
    this.config = null;
  }

  @Inject
  public DefaultServiceIdentityProvider(
      RealmContext realmContext, ServiceIdentityConfiguration serviceIdentityConfiguration) {
    ServiceIdentityConfiguration.RealmConfigEntry entry =
        serviceIdentityConfiguration.forRealm(realmContext);
    this.realm = entry.realm();
    this.config = entry.config();
  }

  @Override
  public Optional<ServiceIdentityInfoDpo> allocateServiceIdentity(
      @Nonnull ConnectionConfigInfo connectionConfig) {
    if (config == null || connectionConfig.getAuthenticationParameters() == null) {
      return Optional.empty();
    }

    AuthenticationParameters.AuthenticationTypeEnum authType =
        connectionConfig.getAuthenticationParameters().getAuthenticationType();

    // Map authentication type to service identity type and check if configured
    return switch (authType) {
      case SIGV4 ->
          config.awsIamServiceIdentity().isPresent()
              ? Optional.of(
                  new AwsIamServiceIdentityInfoDpo(
                      buildIdentityInfoReference(realm, ServiceIdentityType.AWS_IAM)))
              : Optional.empty();
      default -> Optional.empty();
    };
  }

  @Override
  public Optional<ServiceIdentityInfo> getServiceIdentityInfo(
      @Nonnull ServiceIdentityInfoDpo serviceIdentityInfo) {
    if (config == null) {
      return Optional.empty();
    }

    // Find the configuration matching the reference and return metadata only
    SecretReference actualRef = serviceIdentityInfo.getIdentityInfoReference();

    return config.serviceIdentityConfigurations().stream()
        .filter(
            identityConfig ->
                buildIdentityInfoReference(realm, identityConfig.getType()).equals(actualRef))
        .findFirst()
        .flatMap(ResolvableServiceIdentityConfiguration::asServiceIdentityInfoModel);
  }

  @Override
  public Optional<ServiceIdentityCredential> getServiceIdentityCredential(
      @Nonnull ServiceIdentityInfoDpo serviceIdentityInfo) {
    if (config == null) {
      return Optional.empty();
    }

    // Find the configuration matching the reference and resolve credential lazily
    SecretReference ref = serviceIdentityInfo.getIdentityInfoReference();

    return config.serviceIdentityConfigurations().stream()
        .filter(
            identityConfig ->
                buildIdentityInfoReference(realm, identityConfig.getType()).equals(ref))
        .findFirst()
        .flatMap(identityConfig -> identityConfig.asServiceIdentityCredential(ref));
  }

  @VisibleForTesting
  public RealmServiceIdentityConfiguration getRealmConfig() {
    return config;
  }

  /**
   * Builds a {@link SecretReference} for the given realm and service identity type.
   *
   * <p>The URN format is: urn:polaris-secret:default-identity-provider:&lt;realm&gt;:&lt;type&gt;
   *
   * <p>If the realm is the default realm key, it is replaced with "system:default" in the URN.
   *
   * @param realm the realm identifier
   * @param type the service identity type
   * @return the constructed secret reference for this service identity
   */
  public static SecretReference buildIdentityInfoReference(String realm, ServiceIdentityType type) {
    // urn:polaris-secret:default-identity-provider:<realm>:<type>
    return new SecretReference(
        IDENTITY_INFO_REFERENCE_URN_FORMAT.formatted(
            realm.equals(DEFAULT_REALM_KEY) ? DEFAULT_REALM_NSS : realm, type.name()),
        Map.of());
  }
}

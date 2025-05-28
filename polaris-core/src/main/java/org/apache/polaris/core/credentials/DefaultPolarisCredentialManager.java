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

package org.apache.polaris.core.credentials;

import java.util.EnumMap;
import java.util.Optional;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.SigV4AuthenticationParametersDpo;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialProperty;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.registry.ServiceIdentityRegistry;
import org.apache.polaris.core.identity.resolved.ResolvedAwsIamServiceIdentity;
import org.apache.polaris.core.identity.resolved.ResolvedServiceIdentity;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

public class DefaultPolarisCredentialManager implements PolarisCredentialManager {
  private final ServiceIdentityRegistry serviceIdentityRegistry;

  public DefaultPolarisCredentialManager(ServiceIdentityRegistry serviceIdentityRegistry) {
    this.serviceIdentityRegistry = serviceIdentityRegistry;
  }

  @Override
  public EnumMap<ConnectionCredentialProperty, String> getConnectionCredentials(
      ServiceIdentityInfoDpo serviceIdentity,
      AuthenticationParametersDpo authenticationParameters) {
    EnumMap<ConnectionCredentialProperty, String> credentialMap =
        new EnumMap<>(ConnectionCredentialProperty.class);
    ResolvedServiceIdentity resolvedServiceIdentity =
        serviceIdentityRegistry.resolveServiceIdentity(serviceIdentity);
    if (resolvedServiceIdentity == null) {
      return credentialMap;
    }

    switch (serviceIdentity.getIdentityType()) {
      case AWS_IAM:
        ResolvedAwsIamServiceIdentity resolvedAwsIamServiceIdentity =
            (ResolvedAwsIamServiceIdentity) resolvedServiceIdentity;
        SigV4AuthenticationParametersDpo sigV4AuthenticationParameters =
            (SigV4AuthenticationParametersDpo) authenticationParameters;
        StsClient stsClient = resolvedAwsIamServiceIdentity.stsClientSupplier().get();
        AssumeRoleResponse response =
            stsClient.assumeRole(
                AssumeRoleRequest.builder()
                    .roleArn(sigV4AuthenticationParameters.getRoleArn())
                    .roleSessionName(
                        Optional.ofNullable(sigV4AuthenticationParameters.getRoleSessionName())
                            .orElse("polaris"))
                    .externalId(sigV4AuthenticationParameters.getExternalId())
                    .build());
        credentialMap.put(
            ConnectionCredentialProperty.AWS_ACCESS_KEY_ID, response.credentials().accessKeyId());
        credentialMap.put(
            ConnectionCredentialProperty.AWS_SECRET_ACCESS_KEY,
            response.credentials().secretAccessKey());
        credentialMap.put(
            ConnectionCredentialProperty.AWS_SESSION_TOKEN, response.credentials().sessionToken());
        Optional.ofNullable(response.credentials().expiration())
            .ifPresent(
                i -> {
                  credentialMap.put(
                      ConnectionCredentialProperty.EXPIRATION_TIME,
                      String.valueOf(i.toEpochMilli()));
                });
        break;
      default:
        LoggerFactory.getLogger(DefaultPolarisCredentialManager.class)
            .warn("Unsupported service identity type: {}", serviceIdentity.getIdentityType());
        return credentialMap;
    }
    return credentialMap;
  }
}

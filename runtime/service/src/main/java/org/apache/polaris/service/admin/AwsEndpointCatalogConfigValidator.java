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
package org.apache.polaris.service.admin;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.SigV4AuthenticationParameters;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates AWS endpoint settings on every catalog create and update: the endpoint,
 * endpointInternal and stsEndpoint of AWS storage configurations, and the stsEndpoint of SigV4
 * connection configurations. When ALLOW_SETTING_S3_ENDPOINTS is false, it also rejects custom
 * endpoints and disabled STS.
 */
@ApplicationScoped
public class AwsEndpointCatalogConfigValidator implements CatalogConfigValidator {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AwsEndpointCatalogConfigValidator.class);

  private final RealmConfig realmConfig;

  @Inject
  public AwsEndpointCatalogConfigValidator(RealmConfig realmConfig) {
    this.realmConfig = realmConfig;
  }

  @Override
  public void validate(Catalog catalog) {
    AwsStorageConfigInfo awsConfig =
        catalog.getStorageConfigInfo() instanceof AwsStorageConfigInfo aws ? aws : null;
    String sigV4StsEndpoint = null;
    if (catalog instanceof ExternalCatalog externalCatalog
        && externalCatalog.getConnectionConfigInfo() != null
        && externalCatalog.getConnectionConfigInfo().getAuthenticationParameters()
            instanceof SigV4AuthenticationParameters sigV4Parameters) {
      sigV4StsEndpoint = sigV4Parameters.getStsEndpoint();
    }

    // Checked against the merged catalog on every create and update.
    if (!realmConfig.getConfig(FeatureConfiguration.ALLOW_SETTING_S3_ENDPOINTS)) {
      if (awsConfig != null
          && (awsConfig.getEndpoint() != null
              || awsConfig.getEndpointInternal() != null
              || awsConfig.getStsEndpoint() != null)) {
        throw new IllegalArgumentException(
            "This catalog sets custom S3 endpoints, which the realm does not allow.");
      }
      if (sigV4StsEndpoint != null) {
        throw new IllegalArgumentException(
            "This catalog sets a custom SigV4 STS endpoint, which the realm does not allow.");
      }
      if (awsConfig != null && awsConfig.getStsUnavailable() != null) {
        throw new IllegalArgumentException(
            "This catalog disables STS, which the realm does not allow.");
      }
    }

    if (!realmConfig.getConfig(FeatureConfiguration.VALIDATE_AWS_ENDPOINT_URIS)) {
      return;
    }

    String catalogName = catalog.getName();
    if (awsConfig != null) {
      validateEndpointUri(catalogName, "storageConfigInfo.endpoint", awsConfig.getEndpoint());
      validateEndpointUri(
          catalogName, "storageConfigInfo.endpointInternal", awsConfig.getEndpointInternal());
      validateEndpointUri(catalogName, "storageConfigInfo.stsEndpoint", awsConfig.getStsEndpoint());
    }
    validateEndpointUri(catalogName, "authenticationParameters.stsEndpoint", sigV4StsEndpoint);
  }

  private static void validateEndpointUri(String catalogName, String fieldName, String endpoint) {
    if (endpoint == null) {
      return;
    }
    URI uri;
    try {
      uri = new URI(endpoint);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(fieldName + " is not a valid URI.", e);
    }
    String scheme = uri.getScheme();
    if (!("https".equalsIgnoreCase(scheme) || "http".equalsIgnoreCase(scheme))
        || uri.getHost() == null) {
      throw new IllegalArgumentException(fieldName + " must be an absolute http or https URI.");
    }
    if ("http".equalsIgnoreCase(scheme)) {
      LOGGER.warn(
          "Catalog {}: {} uses plain http; requests sent to it are not encrypted.",
          catalogName,
          fieldName);
    }
  }
}

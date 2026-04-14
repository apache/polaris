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
package org.apache.polaris.extension.auth.ranger;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.embedded.RangerEmbeddedAuthorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Identifier("ranger")
public class RangerPolarisAuthorizerFactory implements PolarisAuthorizerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(RangerPolarisAuthorizerFactory.class);

  private static final String ERR_AUTHORIZER_FACTORY_NOT_INITIALIZED =
      "Ranger authorizer factory was not initialized successfully";

  private final RangerPolarisAuthorizerConfig config;
  private RangerEmbeddedAuthorizer authorizer;
  private String serviceName;
  @Inject private RealmContext realmContext;

  @Inject
  RangerPolarisAuthorizerFactory(RangerPolarisAuthorizerConfig config) {
    this.config = config;
    LOG.info("Initializing RangerAuthorizer");
    try {
      Properties properties = config.toRangerProperties();
      RangerEmbeddedAuthorizer authorizer = new RangerEmbeddedAuthorizer(properties);
      authorizer.init();
      this.authorizer = authorizer;
      this.serviceName = config.serviceName();
    } catch (RangerAuthzException t) {
      throw new RuntimeException("Failed to initialize RangerPolarisAuthorizer", t);
    }
    LOG.info("RangerAuthorizer initialized successfully");
    LOG.debug("RangerPolarisAuthorizerFactory has been activated.");
  }

  @Override
  public RangerPolarisAuthorizer create(RealmConfig realmConfig) {
    LOG.debug("Creating RangerPolarisAuthorizer");

    if (authorizer == null || StringUtils.isBlank(serviceName)) {
      throw new IllegalStateException(ERR_AUTHORIZER_FACTORY_NOT_INITIALIZED);
    }

    RangerPolarisAuthorizer polarisAuthorizer =
        new RangerPolarisAuthorizer(authorizer, serviceName, realmConfig);

    if (realmContext != null) {
      polarisAuthorizer.setRealmContext(realmContext);
    }

    return polarisAuthorizer;
  }
}

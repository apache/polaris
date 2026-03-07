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
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Properties;
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.extension.auth.ranger.utils.RangerUtils;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.embedded.RangerEmbeddedAuthorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Identifier("ranger")
public class RangerPolarisAuthorizerFactory implements PolarisAuthorizerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(RangerPolarisAuthorizerFactory.class);

  public static final String SERVICE_NAME_PROPERTY = "ranger.plugin.polaris.service.name";

  private final RangerPolarisAuthorizerConfig config;

  private RangerEmbeddedAuthorizer authorizer;
  private String serviceName;

  @Inject
  RangerPolarisAuthorizerFactory(RangerPolarisAuthorizerConfig config) {
    this.config = config;

    LOG.debug("RangerPolarisAuthorizerFactory has been activated.");
  }

  @PostConstruct
  public void initialize() {
    LOG.info("Initializing RangerAuthorizer");

    config.validate();

    try {
      Properties rangerProp = RangerUtils.loadProperties(config.configFileName().get());
      RangerEmbeddedAuthorizer authorizer = new RangerEmbeddedAuthorizer(rangerProp);

      authorizer.init();

      this.authorizer = authorizer;
      this.serviceName = rangerProp.getProperty(SERVICE_NAME_PROPERTY);
    } catch (RangerAuthzException t) {
      LOG.error("Failed to initialize RangerPolarisAuthorizer", t);
      throw new RuntimeException(t);
    }

    LOG.info("RangerAuthorizer initialized successfully");
  }

  @PreDestroy
  public void cleanup() {}

  @Override
  public RangerPolarisAuthorizer create(RealmConfig realmConfig) {
    LOG.debug("Creating RangerPolarisAuthorizer");

    try {
      return new RangerPolarisAuthorizer(authorizer, serviceName, realmConfig);
    } catch (Throwable t) {
      LOG.error("Failed to create RangerPolarisAuthorizer", t);

      throw t;
    }
  }
}

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
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.core.config.RealmConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Identifier("ranger")
public class RangerPolarisAuthorizerFactory implements PolarisAuthorizerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(RangerPolarisAuthorizerFactory.class);

  private final RangerPolarisAuthorizerConfig config;

  @Inject
  RangerPolarisAuthorizerFactory(RangerPolarisAuthorizerConfig config) {
    this.config = config;

    LOG.debug("RangerPolarisAuthorizerFactory has been activated.");
  }

  @PostConstruct
  public void initialize() {
    config.validate();
  }

  @PreDestroy
  public void cleanup() {}

  @Override
  public RangerPolarisAuthorizer create(RealmConfig realmConfig) {
    LOG.debug("Creating RangerPolarisAuthorizer");

    try {
      return new RangerPolarisAuthorizer(config, realmConfig);
    } catch (Throwable t) {
      LOG.error("Failed to create RangerPolarisAuthorizer", t);
    }

    return null;
  }
}

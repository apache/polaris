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

package org.apache.polaris.service.credentials;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.credentials.DefaultPolarisCredentialManager;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.PolarisCredentialManagerFactory;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;

@ApplicationScoped
@Identifier("default")
public class DefaultPolarisCredentialManagerFactory implements PolarisCredentialManagerFactory {
  private final Map<String, PolarisCredentialManager> cachedCredentialManagers =
      new ConcurrentHashMap<>();

  @Inject
  public DefaultPolarisCredentialManagerFactory() {}

  @Override
  public PolarisCredentialManager getOrCreatePolarisCredentialManager(
      RealmContext realmContext, ServiceIdentityProvider serviceIdentityProvider) {
    return cachedCredentialManagers.computeIfAbsent(
        realmContext.getRealmIdentifier(),
        key -> new DefaultPolarisCredentialManager(serviceIdentityProvider));
  }
}

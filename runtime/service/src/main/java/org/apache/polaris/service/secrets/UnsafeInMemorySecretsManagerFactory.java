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
package org.apache.polaris.service.secrets;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.secrets.UnsafeInMemorySecretsManager;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.secrets.UserSecretsManagerFactory;

@ApplicationScoped
@Identifier("in-memory")
public class UnsafeInMemorySecretsManagerFactory implements UserSecretsManagerFactory {
  private final Map<String, UserSecretsManager> cachedSecretsManagers = new ConcurrentHashMap<>();

  @Override
  public UserSecretsManager getOrCreateUserSecretsManager(RealmContext realmContext) {
    return cachedSecretsManagers.computeIfAbsent(
        realmContext.getRealmIdentifier(), key -> new UnsafeInMemorySecretsManager());
  }
}

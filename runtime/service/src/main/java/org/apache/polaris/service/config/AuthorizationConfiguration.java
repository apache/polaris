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
package org.apache.polaris.service.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithDefaults;
import java.util.Map;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.docs.ConfigDocs;

@ConfigMapping(prefix = "polaris.authorization")
public interface AuthorizationConfiguration {

  /** Default authorizer type used when a realm does not define an override. */
  @WithDefault("internal")
  String type();

  /** Realm-specific authorizer type overrides under {@code polaris.authorization.realms.*}. */
  @WithDefaults
  @ConfigDocs.ConfigPropertyName("realm")
  Map<String, AuthorizationRealmConfiguration> realms();

  default AuthorizationRealmConfiguration forRealm(RealmContext realmContext) {
    return forRealm(realmContext.getRealmIdentifier());
  }

  default AuthorizationRealmConfiguration forRealm(String realmIdentifier) {
    return realms().getOrDefault(realmIdentifier, this::type);
  }
}

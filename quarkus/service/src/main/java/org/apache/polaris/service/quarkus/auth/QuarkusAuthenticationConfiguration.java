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
package org.apache.polaris.service.quarkus.auth;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefaults;
import io.smallrye.config.WithParentName;
import io.smallrye.config.WithUnnamedKey;
import java.util.Map;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.auth.AuthenticationConfiguration;

@ConfigMapping(prefix = "polaris.authentication")
public interface QuarkusAuthenticationConfiguration extends AuthenticationConfiguration {

  @WithParentName
  @WithUnnamedKey(DEFAULT_REALM_KEY)
  @WithDefaults
  @Override
  Map<String, QuarkusAuthenticationRealmConfiguration> realms();

  @Override
  default QuarkusAuthenticationRealmConfiguration forRealm(RealmContext realmContext) {
    return (QuarkusAuthenticationRealmConfiguration)
        AuthenticationConfiguration.super.forRealm(realmContext);
  }
}

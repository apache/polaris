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
package org.apache.polaris.core.connection;

import com.google.common.base.MoreObjects;
import java.util.Map;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.ImplicitAuthenticationParameters;
import org.apache.polaris.core.secrets.UserSecretsManager;

/**
 * The internal persistence-object counterpart to ImplicitAuthenticationParameters defined in the
 * API model.
 */
public class ImplicitAuthenticationParametersDpo extends AuthenticationParametersDpo {

  public ImplicitAuthenticationParametersDpo() {
    super(AuthenticationType.IMPLICIT.getCode());
  }

  @Override
  public Map<String, String> asIcebergCatalogProperties(UserSecretsManager secretsManager) {
    return Map.of();
  }

  @Override
  public AuthenticationParameters asAuthenticationParametersModel() {
    return ImplicitAuthenticationParameters.builder()
        .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.IMPLICIT)
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("authenticationTypeCode", getAuthenticationTypeCode())
        .toString();
  }
}

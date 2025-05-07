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
package org.apache.polaris.service.auth;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A specialized {@link PrincipalAuthInfo} used for internal authentication, when Polaris is the
 * identity provider.
 */
public interface DecodedToken extends PrincipalAuthInfo {

  String getClientId();

  String getSub();

  String getScope();

  @Override
  default String getPrincipalName() {
    // Polaris stores the principal ID in the "sub" claim as a string,
    // and in the "principal_id" claim as a numeric value. It doesn't store
    // the principal name in the token, so we return null here.
    return null;
  }

  @Override
  default Set<String> getPrincipalRoles() {
    // Polaris stores the principal roles in the "scope" claim
    return Arrays.stream(getScope().split(" ")).collect(Collectors.toSet());
  }
}

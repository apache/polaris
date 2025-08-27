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
package org.apache.polaris.service.it.env;

import org.apache.polaris.core.admin.model.PrincipalWithCredentialsCredentials;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;

/**
 * This class holds credentials for accessing the test Polaris Server. An instance of this class
 * representing an admin user is injected into test parameters by {@link
 * PolarisIntegrationTestExtension}.
 */
public record ClientCredentials(String clientId, String clientSecret) {

  /**
   * Creates a {@link ClientCredentials} from an instance of the Admin API model {@link
   * PrincipalWithCredentialsCredentials}.
   */
  public ClientCredentials(PrincipalWithCredentialsCredentials credentials) {
    this(credentials.getClientId(), credentials.getClientSecret());
  }
}

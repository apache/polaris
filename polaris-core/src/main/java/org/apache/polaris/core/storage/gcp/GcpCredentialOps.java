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
package org.apache.polaris.core.storage.gcp;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.IamCredentialsSettings;
import java.io.IOException;

/**
 * Pluggable strategy for the two GCP API calls that vending makes: building an {@link
 * IamCredentialsClient} for impersonation and refreshing a {@link DownscopedCredentials} into an
 * {@link AccessToken}. Production uses {@link #DEFAULT}; tests can supply fakes.
 */
public interface GcpCredentialOps {

  GcpCredentialOps DEFAULT = new GcpCredentialOps() {};

  default IamCredentialsClient createIamCredentialsClient(GoogleCredentials credentials)
      throws IOException {
    return IamCredentialsClient.create(
        IamCredentialsSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build());
  }

  default AccessToken refreshAccessToken(DownscopedCredentials credentials) throws IOException {
    return credentials.refreshAccessToken();
  }
}

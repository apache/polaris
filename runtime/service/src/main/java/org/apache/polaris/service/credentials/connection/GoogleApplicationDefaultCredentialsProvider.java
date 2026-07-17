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
package org.apache.polaris.service.credentials.connection;

import com.google.auth.oauth2.GoogleCredentials;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.util.List;

/**
 * Validates the server-side Google identity used for outbound GCP-authenticated catalog requests.
 *
 * <p>Polaris relies on Application Default Credentials here so credential material stays in the
 * server environment (for example, an attached service account, workload identity, or
 * {@code GOOGLE_APPLICATION_CREDENTIALS}) and is never persisted in catalog entities.
 */
@ApplicationScoped
public class GoogleApplicationDefaultCredentialsProvider {
  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  public void validateCredentialsAvailable() {
    GoogleCredentials credentials = loadApplicationDefaultCredentials();
    refreshAccessToken(credentials);
  }

  private GoogleCredentials loadApplicationDefaultCredentials() {
    try {
      GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
      return credentials.createScopedRequired()
          ? credentials.createScoped(List.of(CLOUD_PLATFORM_SCOPE))
          : credentials;
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unable to load server-side Google Application Default Credentials for outbound GCP"
              + " catalog authentication. Configure an attached service account, workload"
              + " identity, or the GOOGLE_APPLICATION_CREDENTIALS environment variable on the"
              + " Polaris server.");
    }
  }

  private void refreshAccessToken(GoogleCredentials credentials) {
    try {
      credentials.refreshAccessToken();
    } catch (IOException e) {
      throw new IllegalStateException(
          "Server-side Google Application Default Credentials are configured but unusable for"
              + " outbound GCP catalog authentication. Verify that the Polaris server can obtain"
              + " Google access tokens and that the configured identity has permission to access"
              + " the target catalog.");
    }
  }
}

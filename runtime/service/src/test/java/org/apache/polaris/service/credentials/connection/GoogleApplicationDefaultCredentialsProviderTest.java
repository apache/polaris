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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class GoogleApplicationDefaultCredentialsProviderTest {
  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  private final GoogleApplicationDefaultCredentialsProvider provider =
      new GoogleApplicationDefaultCredentialsProvider();

  @Test
  public void validateCredentialsAvailableUsesScopedApplicationDefaultCredentials()
      throws Exception {
    GoogleCredentials defaultCredentials = mock(GoogleCredentials.class);
    GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);

    when(defaultCredentials.createScopedRequired()).thenReturn(true);
    when(defaultCredentials.createScoped(List.of(CLOUD_PLATFORM_SCOPE))).thenReturn(scopedCredentials);
    when(scopedCredentials.refreshAccessToken())
        .thenReturn(new AccessToken("token", new Date(System.currentTimeMillis() + 60_000)));

    try (MockedStatic<GoogleCredentials> mockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {
      mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(defaultCredentials);

      provider.validateCredentialsAvailable();

      mockedStatic.verify(GoogleCredentials::getApplicationDefault, times(1));
      verify(defaultCredentials).createScopedRequired();
      verify(defaultCredentials).createScoped(List.of(CLOUD_PLATFORM_SCOPE));
      verify(scopedCredentials).refreshAccessToken();
    }
  }

  @Test
  public void validateCredentialsAvailableSanitizesCredentialLoadErrors() {
    try (MockedStatic<GoogleCredentials> mockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {
      mockedStatic
          .when(GoogleCredentials::getApplicationDefault)
          .thenThrow(new IOException("missing /var/run/secrets/google/service-account.json"));

      assertThatThrownBy(provider::validateCredentialsAvailable)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("GOOGLE_APPLICATION_CREDENTIALS")
          .hasMessageNotContaining("service-account.json");
    }
  }

  @Test
  public void validateCredentialsAvailableSanitizesCredentialRefreshErrors() throws Exception {
    GoogleCredentials defaultCredentials = mock(GoogleCredentials.class);

    when(defaultCredentials.createScopedRequired()).thenReturn(false);
    when(defaultCredentials.refreshAccessToken())
        .thenThrow(new IOException("token fetch failed for secret-project"));

    try (MockedStatic<GoogleCredentials> mockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {
      mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(defaultCredentials);

      assertThatThrownBy(provider::validateCredentialsAvailable)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("configured but unusable")
          .hasMessageNotContaining("secret-project");
    }
  }
}

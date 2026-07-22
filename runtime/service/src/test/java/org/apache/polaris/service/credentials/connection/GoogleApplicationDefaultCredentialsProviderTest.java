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
import java.util.stream.Stream;
import org.apache.polaris.service.exception.BigLakeFailureCategory;
import org.apache.polaris.service.exception.BigLakeFederationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
          .isInstanceOf(BigLakeFederationException.class)
          .satisfies(
              throwable -> {
                BigLakeFederationException exception = (BigLakeFederationException) throwable;
                org.assertj.core.api.Assertions.assertThat(exception.category())
                    .isEqualTo(BigLakeFailureCategory.CREDENTIAL_LOADING);
                org.assertj.core.api.Assertions.assertThat(exception.getMessage())
                    .contains("GOOGLE_APPLICATION_CREDENTIALS")
                    .doesNotContain("service-account.json");
              });
    }
  }

  static Stream<Arguments> credentialRefreshFailures() {
    return Stream.of(
        Arguments.of(false, "invalid_grant: token expired for secret-project", "secret-project"),
        Arguments.of(true, "credentials revoked: private_key_id=abc123", "abc123"));
  }

  @ParameterizedTest
  @MethodSource
  public void validateCredentialsAvailableSanitizesCredentialRefreshErrors(
      boolean scopedCredentialsRequired, String refreshFailureMessage, String sensitiveFragment)
      throws Exception {
    GoogleCredentials defaultCredentials = mock(GoogleCredentials.class);
    GoogleCredentials refreshCredentials = defaultCredentials;

    when(defaultCredentials.createScopedRequired()).thenReturn(scopedCredentialsRequired);
    if (scopedCredentialsRequired) {
      GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
      when(defaultCredentials.createScoped(List.of(CLOUD_PLATFORM_SCOPE))).thenReturn(scopedCredentials);
      refreshCredentials = scopedCredentials;
    }
    when(refreshCredentials.refreshAccessToken()).thenThrow(new IOException(refreshFailureMessage));

    try (MockedStatic<GoogleCredentials> mockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {
      mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(defaultCredentials);

      assertThatThrownBy(provider::validateCredentialsAvailable)
          .isInstanceOf(BigLakeFederationException.class)
          .satisfies(
              throwable -> {
                BigLakeFederationException exception = (BigLakeFederationException) throwable;
                org.assertj.core.api.Assertions.assertThat(exception.category())
                    .isEqualTo(BigLakeFailureCategory.TOKEN_ACQUISITION);
                org.assertj.core.api.Assertions.assertThat(exception.getMessage())
                    .contains("configured but unusable")
                    .doesNotContain(sensitiveFragment);
              });

      if (scopedCredentialsRequired) {
        verify(defaultCredentials).createScoped(List.of(CLOUD_PLATFORM_SCOPE));
      }
      verify(refreshCredentials).refreshAccessToken();
    }
  }
}

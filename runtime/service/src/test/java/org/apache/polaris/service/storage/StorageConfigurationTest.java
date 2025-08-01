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
package org.apache.polaris.service.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

public class StorageConfigurationTest {

  private static final String TEST_ACCESS_KEY = "test-access-key";
  private static final String TEST_GCP_TOKEN = "ya29.test-token";
  private static final String TEST_SECRET_KEY = "test-secret-key";
  private static final Duration TEST_TOKEN_LIFESPAN = Duration.ofMinutes(20);

  private StorageConfiguration configWithAwsCredentialsAndGcpToken() {
    return new StorageConfiguration() {
      @Override
      public Optional<String> awsAccessKey() {
        return Optional.of(TEST_ACCESS_KEY);
      }

      @Override
      public Optional<String> awsSecretKey() {
        return Optional.of(TEST_SECRET_KEY);
      }

      @Override
      public Optional<String> gcpAccessToken() {
        return Optional.of(TEST_GCP_TOKEN);
      }

      @Override
      public Optional<Duration> gcpAccessTokenLifespan() {
        return Optional.of(TEST_TOKEN_LIFESPAN);
      }

      @Override
      public OptionalInt clientsCacheMaxSize() {
        return OptionalInt.empty();
      }

      @Override
      public OptionalInt maxHttpConnections() {
        return OptionalInt.empty();
      }

      @Override
      public Optional<Duration> readTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionAcquisitionTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionMaxIdleTime() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionTimeToLive() {
        return Optional.empty();
      }

      @Override
      public Optional<Boolean> expectContinueEnabled() {
        return Optional.empty();
      }
    };
  }

  private StorageConfiguration configWithoutGcpToken() {
    return new StorageConfiguration() {
      @Override
      public Optional<String> awsAccessKey() {
        return Optional.empty();
      }

      @Override
      public Optional<String> awsSecretKey() {
        return Optional.empty();
      }

      @Override
      public Optional<String> gcpAccessToken() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> gcpAccessTokenLifespan() {
        return Optional.empty();
      }

      @Override
      public OptionalInt clientsCacheMaxSize() {
        return OptionalInt.empty();
      }

      @Override
      public OptionalInt maxHttpConnections() {
        return OptionalInt.empty();
      }

      @Override
      public Optional<Duration> readTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionAcquisitionTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionMaxIdleTime() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionTimeToLive() {
        return Optional.empty();
      }

      @Override
      public Optional<Boolean> expectContinueEnabled() {
        return Optional.empty();
      }
    };
  }

  @Test
  public void testSingletonStsClientWithStaticCredentials() {
    StsClientBuilder mockBuilder = mock(StsClientBuilder.class);
    StsClient mockStsClient = mock(StsClient.class);
    ArgumentCaptor<StaticCredentialsProvider> credsCaptor =
        ArgumentCaptor.forClass(StaticCredentialsProvider.class);

    when(mockBuilder.credentialsProvider(credsCaptor.capture())).thenReturn(mockBuilder);
    when(mockBuilder.region(any())).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockStsClient);

    try (MockedStatic<StsClient> staticMock = Mockito.mockStatic(StsClient.class)) {
      staticMock.when(StsClient::builder).thenReturn(mockBuilder);

      StorageConfiguration config = configWithAwsCredentialsAndGcpToken();
      Supplier<StsClient> supplier = config.stsClientSupplier();
      StsClient client1 = supplier.get();
      StsClient client2 = supplier.get();

      assertThat(client1).isSameAs(client2);
      assertThat(client1).isNotNull();

      StaticCredentialsProvider credentialsProvider = credsCaptor.getValue();
      assertThat(credentialsProvider.resolveCredentials().accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
      assertThat(credentialsProvider.resolveCredentials().secretAccessKey())
          .isEqualTo(TEST_SECRET_KEY);
    }
  }

  @Test
  public void testStaticStsCredentials() {
    StorageConfiguration config = configWithAwsCredentialsAndGcpToken();
    AwsCredentialsProvider credentialsProvider = config.awsSystemCredentials().orElseThrow();
    assertThat(credentialsProvider).isInstanceOf(StaticCredentialsProvider.class);
    assertThat(credentialsProvider.resolveCredentials().accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
    assertThat(credentialsProvider.resolveCredentials().secretAccessKey())
        .isEqualTo(TEST_SECRET_KEY);
  }

  @Test
  public void testCreateGcpCredentialsFromStaticToken() {
    Clock clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);
    Supplier<GoogleCredentials> supplier =
        configWithAwsCredentialsAndGcpToken().gcpCredentialsSupplier(clock);

    GoogleCredentials credentials = supplier.get();
    assertThat(credentials).isNotNull();

    AccessToken accessToken = credentials.getAccessToken();
    assertThat(accessToken).isNotNull();
    assertThat(accessToken.getTokenValue()).isEqualTo(TEST_GCP_TOKEN);
    assertThat(accessToken.getExpirationTime())
        .isEqualTo(clock.instant().plus(Duration.ofMinutes(20)));
  }

  @Test
  public void testGcpCredentialsFromDefault() {
    GoogleCredentials mockDefaultCreds = mock(GoogleCredentials.class);

    try (MockedStatic<GoogleCredentials> mockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {

      mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockDefaultCreds);

      Supplier<GoogleCredentials> supplier =
          configWithoutGcpToken().gcpCredentialsSupplier(Clock.systemUTC());
      GoogleCredentials result = supplier.get();

      assertThat(result).isSameAs(mockDefaultCreds);
      mockedStatic.verify(GoogleCredentials::getApplicationDefault, times(1));
    }
  }
}

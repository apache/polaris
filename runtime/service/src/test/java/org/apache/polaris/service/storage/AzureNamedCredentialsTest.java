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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.exception.ClientAuthenticationException;
import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.UserDelegationKey;
import com.google.common.base.Throwables;
import io.smallrye.config.ConfigValidationException;
import io.smallrye.config.EnvConfigSource;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.azure.AzureCredentialsStorageIntegration;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.service.storage.StorageConfiguration.AzureNamedCredential;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Named Azure credentials from configuration properties to tokens. These tests use the real {@code
 * ClientSecretCredential}. Only the HTTP calls to Microsoft Entra are faked.
 */
class AzureNamedCredentialsTest {

  private static final String TENANT = "11111111-1111-1111-1111-111111111111";
  private static final String OTHER_TENANT = "22222222-2222-2222-2222-222222222222";
  private static final String SECRET_A = "secret-value-of-team-a";
  private static final String SECRET_B = "secret-value-of-team-b";
  private static final String LOCATION = "wasbs://container@account.blob.core.windows.net/path";

  private static StorageConfiguration configFrom(Map<String, String> properties) {
    return configFrom(properties, Map.of());
  }

  private static StorageConfiguration configFrom(
      Map<String, String> properties, Map<String, String> environment) {
    return new SmallRyeConfigBuilder()
        .withMapping(StorageConfiguration.class)
        .withSources(new PropertiesConfigSource(properties, "test", 100))
        .withSources(new EnvConfigSource(environment, 300))
        .build()
        .getConfigMapping(StorageConfiguration.class);
  }

  private static Map<String, String> properties(String name, String clientSecret) {
    return Map.of(
        "polaris.storage.azure." + name + ".tenant-id", TENANT,
        "polaris.storage.azure." + name + ".client-id", "app-" + name,
        "polaris.storage.azure." + name + ".client-secret", clientSecret);
  }

  private static Map<String, String> twoNames() {
    Map<String, String> properties = new HashMap<>(properties("team-a", SECRET_A));
    properties.putAll(properties("team-b", SECRET_B));
    return properties;
  }

  /** Answers like Microsoft Entra, and records each token request. */
  private static final class FakeEntra implements HttpClient {
    private final List<Map<String, String>> tokenRequests = new CopyOnWriteArrayList<>();
    private final List<String> tokenPaths = new CopyOnWriteArrayList<>();
    private final int expiresInSeconds;
    private final boolean rejectSecrets;

    FakeEntra(int expiresInSeconds, boolean rejectSecrets) {
      this.expiresInSeconds = expiresInSeconds;
      this.rejectSecrets = rejectSecrets;
    }

    FakeEntra() {
      this(3600, false);
    }

    @Override
    public Mono<HttpResponse> send(HttpRequest request) {
      if (request.getUrl().getPath().endsWith("/discovery/instance")) {
        return respond(
            request,
            200,
            "{\"tenant_discovery_endpoint\":"
                + "\"https://login.microsoftonline.com/t/v2.0/.well-known/openid-configuration\","
                + "\"metadata\":[{\"preferred_network\":\"login.microsoftonline.com\","
                + "\"preferred_cache\":\"login.windows.net\","
                + "\"aliases\":[\"login.microsoftonline.com\",\"login.windows.net\"]}]}");
      }
      Map<String, String> form = parseForm(request.getBodyAsBinaryData().toString());
      tokenRequests.add(form);
      tokenPaths.add(request.getUrl().getPath());
      if (rejectSecrets) {
        return respond(
            request,
            401,
            "{\"error\":\"invalid_client\",\"error_description\":\"AADSTS7000215: Invalid client"
                + " secret provided.\",\"error_codes\":[7000215]}");
      }
      return respond(
          request,
          200,
          "{\"token_type\":\"Bearer\",\"expires_in\":"
              + expiresInSeconds
              + ",\"access_token\":\"token-for-"
              + form.get("client_id")
              + "-"
              + tokenRequests.size()
              + "\"}");
    }

    private static Map<String, String> parseForm(String form) {
      return Arrays.stream(form.split("&"))
          .map(pair -> pair.split("=", 2))
          .collect(Collectors.toMap(p -> p[0], p -> URLDecoder.decode(p[1], UTF_8)));
    }

    private static Mono<HttpResponse> respond(HttpRequest request, int status, String body) {
      return Mono.just(new FakeResponse(request, status, body));
    }
  }

  private static final class FakeResponse extends HttpResponse {
    private final int status;
    private final String body;

    FakeResponse(HttpRequest request, int status, String body) {
      super(request);
      this.status = status;
      this.body = body;
    }

    @Override
    public int getStatusCode() {
      return status;
    }

    @Override
    @SuppressWarnings("deprecation")
    public String getHeaderValue(String name) {
      return getHeaders().getValue(name);
    }

    @Override
    public HttpHeaders getHeaders() {
      return new HttpHeaders().set(HttpHeaderName.CONTENT_TYPE, "application/json");
    }

    @Override
    public Flux<ByteBuffer> getBody() {
      return Flux.just(ByteBuffer.wrap(body.getBytes(UTF_8)));
    }

    @Override
    public Mono<byte[]> getBodyAsByteArray() {
      return Mono.just(body.getBytes(UTF_8));
    }

    @Override
    public Mono<String> getBodyAsString() {
      return Mono.just(body);
    }

    @Override
    public Mono<String> getBodyAsString(Charset charset) {
      return Mono.just(body);
    }
  }

  /**
   * Builds a named credential through the production code, with {@code entra} as its HTTP client.
   */
  private static TokenCredential credential(AzureNamedCredential named, HttpClient entra) {
    ClientSecretCredentialBuilder realBuilder =
        new ClientSecretCredentialBuilder().httpClient(entra);
    try (MockedConstruction<ClientSecretCredentialBuilder> ignored =
        Mockito.mockConstruction(
            ClientSecretCredentialBuilder.class,
            Mockito.withSettings()
                .defaultAnswer(
                    call -> call.getMethod().invoke(realBuilder, call.getArguments())))) {
      return named.credential().get();
    }
  }

  private static String token(TokenCredential credential, String tenant) {
    return credential
        .getToken(
            new TokenRequestContext()
                .addScopes("https://storage.azure.com/.default")
                .setTenantId(tenant))
        .block()
        .getToken();
  }

  @Test
  void noNamedCredentialsByDefault() {
    assertThat(configFrom(Map.of()).azureNamedCredentials()).isEmpty();
  }

  @Test
  void eachNameSignsInWithItsOwnServicePrincipal() {
    Map<String, AzureNamedCredential> named = configFrom(twoNames()).azureNamedCredentials();
    FakeEntra entra = new FakeEntra();

    String tokenA = token(credential(named.get("team-a"), entra), TENANT);
    String tokenB = token(credential(named.get("team-b"), entra), TENANT);

    assertThat(tokenA).startsWith("token-for-app-team-a-");
    assertThat(tokenB).startsWith("token-for-app-team-b-");
    assertThat(entra.tokenRequests).hasSize(2);
    assertThat(entra.tokenRequests.get(0))
        .containsEntry("client_id", "app-team-a")
        .containsEntry("client_secret", SECRET_A);
    assertThat(entra.tokenRequests.get(1))
        .containsEntry("client_id", "app-team-b")
        .containsEntry("client_secret", SECRET_B);
    assertThat(entra.tokenPaths).allMatch(path -> path.startsWith("/" + TENANT + "/"));
  }

  @Test
  void aNameIsBuiltOnceSoItsTokenCacheIsShared() {
    AzureNamedCredential named =
        configFrom(properties("team-a", SECRET_A)).azureNamedCredentials().get("team-a");
    FakeEntra entra = new FakeEntra();

    TokenCredential first = credential(named, entra);
    assertThat(named.credential().get()).isSameAs(first);
    String token = token(first, TENANT);

    assertThat(token(named.credential().get(), TENANT)).isEqualTo(token);
    assertThat(entra.tokenRequests).hasSize(1);
  }

  @Test
  void anExpiringTokenIsRenewedWithoutAnyAction() {
    AzureNamedCredential named =
        configFrom(properties("team-a", SECRET_A)).azureNamedCredentials().get("team-a");
    FakeEntra entra = new FakeEntra(60, false);
    TokenCredential credential = credential(named, entra);

    String first = token(credential, TENANT);
    String second = token(credential, TENANT);

    assertThat(second).isNotEqualTo(first);
    assertThat(entra.tokenRequests).hasSize(2);
  }

  @Test
  void vendingCallsStorageWithTheTokenOfTheNamedCredential() {
    AzureNamedCredential named =
        configFrom(properties("team-a", SECRET_A)).azureNamedCredentials().get("team-a");
    FakeEntra entra = new FakeEntra();
    TokenCredential credential = credential(named, entra);
    List<TokenCredential> storageCredentials = new ArrayList<>();
    UserDelegationKey key = Mockito.mock(UserDelegationKey.class);
    BlobServiceClient serviceClient = Mockito.mock(BlobServiceClient.class);
    Mockito.when(serviceClient.getUserDelegationKey(Mockito.any(), Mockito.any())).thenReturn(key);
    BlobContainerClient containerClient = Mockito.mock(BlobContainerClient.class);
    Mockito.when(containerClient.generateUserDelegationSas(Mockito.any(), Mockito.same(key)))
        .thenReturn("sas-token");

    try (MockedConstruction<BlobServiceClientBuilder> ignored =
            Mockito.mockConstruction(
                BlobServiceClientBuilder.class,
                Mockito.withSettings().defaultAnswer(Answers.RETURNS_SELF),
                (builder, context) -> {
                  Mockito.when(builder.credential(Mockito.any(TokenCredential.class)))
                      .thenAnswer(
                          call -> {
                            storageCredentials.add(call.getArgument(0));
                            return builder;
                          });
                  Mockito.when(builder.buildClient()).thenReturn(serviceClient);
                });
        MockedConstruction<BlobContainerClientBuilder> ignoredContainer =
            Mockito.mockConstruction(
                BlobContainerClientBuilder.class,
                Mockito.withSettings().defaultAnswer(Answers.RETURNS_SELF),
                (builder, context) ->
                    Mockito.when(builder.buildClient()).thenReturn(containerClient))) {
      new AzureCredentialsStorageIntegration(
              credential,
              null,
              AzureStorageConfigurationInfo.builder()
                  .addAllowedLocation(LOCATION)
                  .tenantId(TENANT)
                  .storageName("team-a")
                  .build(),
              new RealmConfigImpl((realm, name) -> null, () -> "test-realm"))
          .getStorageAccessConfig(
              List.of(new LocationGrant(Set.of(LOCATION), Set.of(PolarisStorageActions.READ))),
              Optional.empty(),
              CredentialVendingContext.empty());
    }

    assertThat(storageCredentials).hasSize(1);
    assertThat(token(storageCredentials.get(0), TENANT)).startsWith("token-for-app-team-a-");
    assertThat(entra.tokenRequests).hasSize(1);
  }

  @Test
  void theCredentialItselfRefusesAnotherTenant() {
    AzureNamedCredential named =
        configFrom(properties("team-a", SECRET_A)).azureNamedCredentials().get("team-a");
    FakeEntra entra = new FakeEntra();
    TokenCredential credential = credential(named, entra);

    assertThatThrownBy(() -> token(credential, OTHER_TENANT))
        .isInstanceOf(ClientAuthenticationException.class);
    assertThat(entra.tokenRequests).isEmpty();
  }

  @Test
  void aRejectedSecretDoesNotAppearInTheFailure() {
    AzureNamedCredential named =
        configFrom(properties("team-a", SECRET_A)).azureNamedCredentials().get("team-a");
    FakeEntra entra = new FakeEntra(3600, true);
    TokenCredential credential = credential(named, entra);

    assertThatThrownBy(() -> token(credential, TENANT))
        .satisfies(
            failure ->
                assertThat(Throwables.getStackTraceAsString(failure))
                    .contains("AADSTS7000215")
                    .doesNotContain(SECRET_A));
    assertThat(entra.tokenRequests)
        .singleElement()
        .extracting(f -> f.get("client_secret"))
        .isEqualTo(SECRET_A);
  }

  @Test
  void aBadEntryFailsOnlyItsOwnNameAndKeepsTheSecretOutOfTheError() {
    Map<String, String> properties = twoNames();
    properties.put("polaris.storage.azure.team-a.tenant-id", "not/a/tenant");
    Map<String, AzureNamedCredential> named = configFrom(properties).azureNamedCredentials();

    assertThatThrownBy(() -> named.get("team-a").credential().get())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("team-a")
        .hasMessageContaining("IllegalArgumentException")
        .hasMessageNotContaining(SECRET_A)
        .hasMessageNotContaining("not/a/tenant")
        .hasNoCause();
    assertThat(named.get("team-b").credential().get()).isNotNull();
  }

  @Test
  void aSecretDoesNotAppearInToString() {
    StorageConfiguration config = configFrom(properties("team-a", SECRET_A));
    AzureNamedCredential named = config.azureNamedCredentials().get("team-a");

    assertThat(named.toString()).doesNotContain(SECRET_A);
    assertThat(config.azure().storages().get("team-a").toString()).doesNotContain(SECRET_A);
    assertThat(config.azure().toString()).doesNotContain(SECRET_A);
    assertThat(config.toString()).doesNotContain(SECRET_A);
    assertThat(named.credential().get().toString()).doesNotContain(SECRET_A);
  }

  @Test
  void anIncompleteEntryFailsAtStartupNamingThePropertyButNoValues() {
    assertThatThrownBy(
            () ->
                configFrom(
                    Map.of(
                        "polaris.storage.azure.team-a.tenant-id", TENANT,
                        "polaris.storage.azure.team-a.client-secret", SECRET_A)))
        .isInstanceOf(ConfigValidationException.class)
        .hasMessageContaining("polaris.storage.azure.team-a.client-id")
        .hasMessageNotContaining(SECRET_A);
  }

  @Test
  void environmentVariableSuppliesTheSecretOfANameDefinedByProperties() {
    Map<String, AzureNamedCredential> named =
        configFrom(
                Map.of(
                    "polaris.storage.azure.team-a.tenant-id",
                    TENANT,
                    "polaris.storage.azure.team-a.client-id",
                    "app-team-a"),
                Map.of("POLARIS_STORAGE_AZURE_TEAM_A_CLIENT_SECRET", SECRET_A))
            .azureNamedCredentials();
    FakeEntra entra = new FakeEntra();

    token(credential(named.get("team-a"), entra), TENANT);

    assertThat(entra.tokenRequests.get(0)).containsEntry("client_secret", SECRET_A);
  }

  @Test
  void environmentVariablesAloneCannotDefineANameWithADash() {
    StorageConfiguration config =
        configFrom(
            Map.of(),
            Map.of(
                "POLARIS_STORAGE_AZURE_TEAM_A_TENANT_ID", TENANT,
                "POLARIS_STORAGE_AZURE_TEAM_A_CLIENT_ID", "app-team-a",
                "POLARIS_STORAGE_AZURE_TEAM_A_CLIENT_SECRET", SECRET_A));

    assertThat(config.azureNamedCredentials()).isEmpty();
  }

  @Test
  void environmentVariablesAloneDefineANameWithoutADash() {
    StorageConfiguration config =
        configFrom(
            Map.of(),
            Map.of(
                "POLARIS_STORAGE_AZURE_TEAMA_TENANT_ID", TENANT,
                "POLARIS_STORAGE_AZURE_TEAMA_CLIENT_ID", "app-teama",
                "POLARIS_STORAGE_AZURE_TEAMA_CLIENT_SECRET", SECRET_A));

    assertThat(config.azureNamedCredentials()).containsOnlyKeys("teama");
  }

  @Test
  void literalPropertyNamesInTheEnvironmentDefineANameWithADash() {
    StorageConfiguration config =
        configFrom(
            Map.of(),
            Map.of(
                "polaris.storage.azure.team-a.tenant-id", TENANT,
                "polaris.storage.azure.team-a.client-id", "app-team-a",
                "polaris.storage.azure.team-a.client-secret", SECRET_A));

    assertThat(config.azureNamedCredentials()).containsOnlyKeys("team-a");
  }
}

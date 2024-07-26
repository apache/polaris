package io.polaris.service.auth;

import static io.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;

public class TokenUtils {

  /** Get token against default realm */
  public static String getTokenFromSecrets(
      Client client, int port, String clientId, String clientSecret) {
    return getTokenFromSecrets(client, port, clientId, clientSecret, null);
  }

  /** Get token against specified realm */
  public static String getTokenFromSecrets(
      Client client, int port, String clientId, String clientSecret, String realm) {
    String token;

    Invocation.Builder builder =
        client
            .target(String.format("http://localhost:%d/api/catalog/v1/oauth/tokens", port))
            .request("application/json");
    if (realm != null) {
      builder = builder.header(REALM_PROPERTY_KEY, realm);
    }

    try (Response response =
        builder.post(
            Entity.form(
                new MultivaluedHashMap<>(
                    Map.of(
                        "grant_type",
                        "client_credentials",
                        "scope",
                        "PRINCIPAL_ROLE:ALL",
                        "client_id",
                        clientId,
                        "client_secret",
                        clientSecret))))) {
      assertThat(response).returns(200, Response::getStatus);
      token = response.readEntity(OAuthTokenResponse.class).token();
    }
    return token;
  }
}

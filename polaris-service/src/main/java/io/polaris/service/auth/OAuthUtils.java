package io.polaris.service.auth;

import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.binary.Base64;

/** Simple utility class to assist with OAuth operations */
public class OAuthUtils {

  public static final String AUTHORIZATION_HEADER = "Authorization";

  public static final String SF_HEADER_ACCOUNT_NAME = "Snowflake-Account";

  public static final String POLARIS_ROLE_PREFIX = "PRINCIPAL_ROLE:";

  public static final String SF_ACCOUNT_NAME_HEADER = "sf-account";
  public static final String SF_ACCOUNT_URL_HEADER = "sf-account-url";

  /**
   * @param clientId
   * @param clientSecret
   * @return basic Authorization Header of the form `base64_encode(client_id:client_secret)
   */
  public static String getBasicAuthHeader(String clientId, String clientSecret) {
    return Base64.encodeBase64String(
        (clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
  }

  public static Response getResponseFromError(OAuthTokenErrorResponse.Error error) {
    return switch (error) {
      case unauthorized_client ->
          Response.status(Response.Status.UNAUTHORIZED)
              .entity(
                  new OAuthTokenErrorResponse(OAuthTokenErrorResponse.Error.unauthorized_client))
              .build();
      case invalid_client ->
          Response.status(Response.Status.BAD_REQUEST)
              .entity(new OAuthTokenErrorResponse(OAuthTokenErrorResponse.Error.invalid_client))
              .build();
      case invalid_grant ->
          Response.status(Response.Status.BAD_REQUEST)
              .entity(new OAuthTokenErrorResponse(OAuthTokenErrorResponse.Error.invalid_grant))
              .build();
      case unsupported_grant_type ->
          Response.status(Response.Status.BAD_REQUEST)
              .entity(
                  new OAuthTokenErrorResponse(OAuthTokenErrorResponse.Error.unsupported_grant_type))
              .build();
      case invalid_scope ->
          Response.status(Response.Status.BAD_REQUEST)
              .entity(new OAuthTokenErrorResponse(OAuthTokenErrorResponse.Error.invalid_scope))
              .build();
      default ->
          Response.status(Response.Status.BAD_REQUEST)
              .entity(new OAuthTokenErrorResponse(OAuthTokenErrorResponse.Error.invalid_request))
              .build();
    };
  }
}

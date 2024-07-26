package io.polaris.service.auth;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.polaris.core.context.CallContext;
import io.polaris.service.config.HasEntityManagerFactory;
import io.polaris.service.config.OAuth2ApiService;
import io.polaris.service.config.RealmEntityManagerFactory;
import io.polaris.service.types.TokenType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;

/**
 * Default implementation of the {@link OAuth2ApiService} that generates a JWT token for the client
 * if the client secret matches.
 */
@JsonTypeName("default")
public class DefaultOAuth2ApiService implements OAuth2ApiService, HasEntityManagerFactory {
  private TokenBrokerFactory tokenBrokerFactory;

  public DefaultOAuth2ApiService() {}

  @Override
  public Response getToken(
      String grantType,
      String scope,
      String clientId,
      String clientSecret,
      TokenType requestedTokenType,
      String subjectToken,
      TokenType subjectTokenType,
      String actorToken,
      TokenType actorTokenType,
      SecurityContext securityContext) {

    TokenBroker tokenBroker =
        tokenBrokerFactory.apply(CallContext.getCurrentContext().getRealmContext());
    if (!tokenBroker.supportsGrantType(grantType)) {
      return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.unsupported_grant_type);
    }
    if (!tokenBroker.supportsRequestedTokenType(requestedTokenType)) {
      return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.invalid_request);
    }
    TokenResponse tokenResponse =
        switch (subjectTokenType) {
          case TokenType.ID_TOKEN,
                  TokenType.REFRESH_TOKEN,
                  TokenType.JWT,
                  TokenType.SAML1,
                  TokenType.SAML2 ->
              new TokenResponse(OAuthTokenErrorResponse.Error.invalid_request);
          case TokenType.ACCESS_TOKEN ->
              tokenBroker.generateFromToken(subjectTokenType, subjectToken, grantType, scope);
          case null ->
              tokenBroker.generateFromClientSecrets(clientId, clientSecret, grantType, scope);
        };
    if (!tokenResponse.isValid()) {
      return OAuthUtils.getResponseFromError(tokenResponse.getError());
    }
    return Response.ok(
            OAuthTokenResponse.builder()
                .withToken(tokenResponse.getAccessToken())
                .withTokenType(OAuth2Constants.BEARER)
                .setExpirationInSeconds(tokenResponse.getExpiresIn())
                .build())
        .build();
  }

  @Override
  public void setEntityManagerFactory(RealmEntityManagerFactory entityManagerFactory) {
    if (tokenBrokerFactory instanceof HasEntityManagerFactory hemf) {
      hemf.setEntityManagerFactory(entityManagerFactory);
    }
  }

  public void setTokenBroker(TokenBrokerFactory tokenBrokerFactory) {
    this.tokenBrokerFactory = tokenBrokerFactory;
  }
}

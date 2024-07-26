package io.polaris.service.auth;

import java.util.Optional;

public class TokenResponse {
  private final Optional<OAuthTokenErrorResponse.Error> error;
  private String accessToken;
  private String tokenType;
  private Integer expiresIn;

  public TokenResponse(OAuthTokenErrorResponse.Error error) {
    this.error = Optional.of(error);
  }

  public TokenResponse(String accessToken, String tokenType, int expiresIn) {
    this.accessToken = accessToken;
    this.expiresIn = expiresIn;
    this.tokenType = tokenType;
    this.error = Optional.empty();
  }

  public boolean isValid() {
    return error.isEmpty();
  }

  public OAuthTokenErrorResponse.Error getError() {
    return error.get();
  }

  public String getAccessToken() {
    return accessToken;
  }

  public int getExpiresIn() {
    return expiresIn;
  }

  public String getTokenType() {
    return tokenType;
  }
}

package io.polaris.service.auth;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TokenInfoExchangeResponse implements DecodedToken {

  private boolean active;

  @JsonProperty("active")
  public boolean isActive() {
    return active;
  }

  @JsonProperty("active")
  public void setActive(boolean active) {
    this.active = active;
  }

  private String scope;

  @JsonProperty("scope")
  public String getScope() {
    return scope;
  }

  @JsonProperty("scope")
  public void setScope(String scope) {
    this.scope = scope;
  }

  private String clientId;

  @JsonProperty("client_id")
  public String getClientId() {
    return clientId;
  }

  @JsonProperty("client_id")
  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  private String tokenType;

  @JsonProperty("token_type")
  public String getTokenType() {
    return tokenType;
  }

  @JsonProperty("token_type")
  public void setTokenType(String tokenType) {
    this.tokenType = tokenType;
  }

  private Long exp;

  @JsonProperty("exp")
  public Long getExp() {
    return exp;
  }

  @JsonProperty("exp")
  public void setExp(Long exp) {
    this.exp = exp;
  }

  private String sub;

  @JsonProperty("sub")
  public String getSub() {
    return sub;
  }

  @JsonProperty("sub")
  public void setSub(String sub) {
    this.sub = sub;
  }

  private String aud;

  @JsonProperty("aud")
  public String getAud() {
    return aud;
  }

  @JsonProperty("aud")
  public void setAud(String aud) {
    this.aud = aud;
  }

  @JsonProperty("iss")
  private String iss;

  @JsonProperty("iss")
  public String getIss() {
    return iss;
  }

  @JsonProperty("iss")
  public void setIss(String iss) {
    this.iss = iss;
  }

  private String token;

  @JsonProperty("token")
  public String getToken() {
    return token;
  }

  @JsonProperty("token")
  public void setToken(String token) {
    this.token = token;
  }

  private long integrationId;

  public long getIntegrationId() {
    return integrationId;
  }

  @JsonProperty("integration_id")
  public void setIntegrationId(long integrationId) {
    this.integrationId = integrationId;
  }

  /* integration ID is effectively principal ID */
  @Override
  public Long getPrincipalId() {
    return integrationId;
  }
}

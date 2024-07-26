package io.polaris.service.auth;

public interface DecodedToken {
  Long getPrincipalId();

  String getClientId();

  String getSub();

  String getScope();
}

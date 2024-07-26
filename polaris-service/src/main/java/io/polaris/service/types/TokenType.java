package io.polaris.service.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Token type identifier, from RFC 8693 Section 3 See
 * https://datatracker.ietf.org/doc/html/rfc8693#section-3
 */
public enum TokenType {
  ACCESS_TOKEN("urn:ietf:params:oauth:token-type:access_token"),

  REFRESH_TOKEN("urn:ietf:params:oauth:token-type:refresh_token"),

  ID_TOKEN("urn:ietf:params:oauth:token-type:id_token"),

  SAML1("urn:ietf:params:oauth:token-type:saml1"),

  SAML2("urn:ietf:params:oauth:token-type:saml2"),

  JWT("urn:ietf:params:oauth:token-type:jwt");

  private String value;

  TokenType(String value) {
    this.value = value;
  }

  @JsonValue
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }

  @JsonCreator
  public static TokenType fromValue(String value) {
    for (TokenType b : TokenType.values()) {
      if (b.value.equals(value)) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }
}

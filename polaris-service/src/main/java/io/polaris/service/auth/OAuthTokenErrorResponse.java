package io.polaris.service.auth;

import com.fasterxml.jackson.annotation.JsonProperty;

/** An OAuth Error Token Response as defined by the Iceberg REST API OpenAPI Spec. */
public class OAuthTokenErrorResponse {

  public enum Error {
    invalid_request("The request is invalid"),
    invalid_client("The Client is invalid"),
    invalid_grant("The grant is invalid"),
    unauthorized_client("The client is not authorized"),
    unsupported_grant_type("The grant type is invalid"),
    invalid_scope("The scope is invalid"),
    ;

    String errorDescription;

    Error(String errorDescription) {
      this.errorDescription = errorDescription;
    }

    public String getErrorDescription() {
      return errorDescription;
    }
  }

  private final String error;
  private final String errorDescription;
  private String errorUri;

  /**
   * Initlaizes a response from one of the supported errors
   *
   * @param error
   */
  public OAuthTokenErrorResponse(Error error) {
    this.error = error.name();
    this.errorDescription = error.getErrorDescription();
    this.errorUri = null; // Not yet used
  }

  @JsonProperty("error")
  public String getError() {
    return error;
  }

  @JsonProperty("error_description")
  public String getErrorDescription() {
    return errorDescription;
  }

  @JsonProperty("error_uri")
  public String getErrorUri() {
    return errorUri;
  }
}

package org.apache.polaris.service.auth;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.OpaPolarisAuthorizer;

@ApplicationScoped
public class AuthorizerProducer {

  @ConfigProperty(name = "polaris.authorization.implementation", defaultValue = "default")
  String impl;

  @ConfigProperty(name = "polaris.authorization.opa.url", defaultValue = "http://localhost:8181")
  String opaUrl;

  @ConfigProperty(name = "polaris.authorization.opa.policyPath", defaultValue = "/v1/data/polaris/allow")
  String policyPath;

  @ConfigProperty(name = "polaris.authorization.opa.timeout-ms", defaultValue = "2000")
  int timeoutMs;

  @Produces
  @ApplicationScoped
  PolarisAuthorizer polarisAuthorizer() {
    if ("opa".equalsIgnoreCase(impl)) {
      return OpaPolarisAuthorizer.create(opaUrl, policyPath, timeoutMs, null, null);
    }
    return new PolarisAuthorizerImpl();
  }
}

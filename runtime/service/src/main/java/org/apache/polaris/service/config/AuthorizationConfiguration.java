package org.apache.polaris.service.config;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "polaris.authorization")
public interface AuthorizationConfiguration {
  String implementation();

  OpaConfig opa();

  interface OpaConfig {
    String url();

    String policyPath();

    int timeoutMs();
  }
}

package io.polaris.service;

import com.codahale.metrics.health.HealthCheck;

/** Default {@link HealthCheck} implementation. */
public class PolarisHealthCheck extends HealthCheck {
  @Override
  protected Result check() throws Exception {
    return Result.healthy();
  }
}

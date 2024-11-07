/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.test;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * JUnit test extension that determines the TestEnvironment. Falls back to targetting the local
 * Dropwizard instance.
 */
public class TestEnvironmentExtension implements ParameterResolver {
  /**
   * Environment variable that specifies the test environment resolver. This should be a class name.
   * If this is not set, falls back to DropwizardTestEnvironmentResolver.
   */
  public static final String ENV_TEST_ENVIRONMENT_RESOLVER_IMPL =
      "INTEGRATION_TEST_ENVIRONMENT_RESOLVER_IMPL";

  private static final String ENV_PROPERTY_KEY = "testenvironment";

  public static TestEnvironment getEnv(ExtensionContext extensionContext)
      throws IllegalAccessException {
    // This must be cached because the TestEnvironment has a randomly generated ID
    return extensionContext
        .getStore(ExtensionContext.Namespace.create(extensionContext.getRequiredTestClass()))
        .getOrComputeIfAbsent(
            ENV_PROPERTY_KEY,
            (k) -> getTestEnvironmentResolver().resolveTestEnvironment(extensionContext),
            TestEnvironment.class);
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType().equals(TestEnvironment.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    try {
      return getEnv(extensionContext);
    } catch (IllegalAccessException e) {
      throw new ParameterResolutionException(e.getMessage());
    }
  }

  private static TestEnvironmentResolver getTestEnvironmentResolver() {
    String impl =
        Optional.ofNullable(System.getenv(ENV_TEST_ENVIRONMENT_RESOLVER_IMPL))
            .orElse(DropwizardTestEnvironmentResolver.class.getName());

    try {
      return (TestEnvironmentResolver) (Class.forName(impl).getDeclaredConstructor().newInstance());
    } catch (InstantiationException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException
        | ClassNotFoundException
        | NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to initialize TestEnvironmentResolver using implementation %s.", impl),
          e);
    }
  }
}

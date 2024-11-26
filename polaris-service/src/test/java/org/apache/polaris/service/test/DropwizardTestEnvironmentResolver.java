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

import io.dropwizard.testing.junit5.DropwizardAppExtension;
import jakarta.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.platform.commons.util.ReflectionUtils;
import org.slf4j.LoggerFactory;

public class DropwizardTestEnvironmentResolver implements TestEnvironmentResolver {
  /**
   * Resolves the TestEnvironment to point to the local Dropwizard instance.
   *
   * @param extensionContext
   * @return
   */
  @Override
  public TestEnvironment resolveTestEnvironment(ExtensionContext extensionContext) {
    try {
      DropwizardAppExtension dropwizardAppExtension = findDropwizardExtension(extensionContext);
      if (dropwizardAppExtension == null) {
        throw new ParameterResolutionException("Could not find DropwizardAppExtension.");
      }
      return new TestEnvironment(
          dropwizardAppExtension.client(),
          String.format("http://localhost:%d", dropwizardAppExtension.getLocalPort()));
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static @Nullable DropwizardAppExtension findDropwizardExtension(
      ExtensionContext extensionContext) throws IllegalAccessException {
    Field dropwizardExtensionField =
        findAnnotatedFields(extensionContext.getRequiredTestClass(), true);
    if (dropwizardExtensionField == null) {
      LoggerFactory.getLogger(PolarisGrantRecord.class)
          .warn(
              "Unable to find dropwizard extension field in test class {}",
              extensionContext.getRequiredTestClass());
      return null;
    }
    DropwizardAppExtension appExtension =
        (DropwizardAppExtension) ReflectionUtils.makeAccessible(dropwizardExtensionField).get(null);
    return appExtension;
  }

  private static Field findAnnotatedFields(Class<?> testClass, boolean isStaticMember) {
    final Optional<Field> set =
        Arrays.stream(testClass.getDeclaredFields())
            .filter(m -> isStaticMember == Modifier.isStatic(m.getModifiers()))
            .filter(m -> DropwizardAppExtension.class.isAssignableFrom(m.getType()))
            .findFirst();
    if (set.isPresent()) {
      return set.get();
    }
    if (!testClass.getSuperclass().equals(Object.class)) {
      return findAnnotatedFields(testClass.getSuperclass(), isStaticMember);
    }
    return null;
  }
}

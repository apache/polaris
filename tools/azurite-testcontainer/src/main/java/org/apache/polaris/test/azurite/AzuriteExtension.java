/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// CODE_COPIED_TO_POLARIS from Project Nessie 0.106.1
package org.apache.polaris.test.azurite;

import static java.lang.String.format;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotatedFields;
import static org.junit.platform.commons.util.ReflectionUtils.makeAccessible;

import java.lang.reflect.Field;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.AnnotationUtils;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;

public class AzuriteExtension
    implements BeforeAllCallback, BeforeEachCallback, ParameterResolver, ExecutionCondition {
  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(AzuriteExtension.class);

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    if (OS.current() == OS.LINUX) {
      return enabled("Running on Linux");
    }
    if (OS.current() == OS.MAC) {
      return enabled("Running on macOS");
    }
    return disabled(format("Disabled on %s", OS.current().name()));
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();

    findAnnotatedFields(testClass, Azurite.class, ReflectionUtils::isStatic)
        .forEach(field -> injectField(context, field));
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();

    findAnnotatedFields(testClass, Azurite.class, ReflectionUtils::isNotStatic)
        .forEach(field -> injectField(context, field));
  }

  private void injectField(ExtensionContext context, Field field) {
    try {
      Azurite azurite =
          AnnotationUtils.findAnnotation(field, Azurite.class)
              .orElseThrow(IllegalStateException::new);

      AzuriteAccess container =
          context
              .getStore(NAMESPACE)
              .getOrComputeIfAbsent(
                  field.toString(), x -> createContainer(azurite), AzuriteAccess.class);

      makeAccessible(field).set(context.getTestInstance().orElse(null), container);
    } catch (Throwable t) {
      ExceptionUtils.throwAsUncheckedException(t);
    }
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    if (parameterContext.findAnnotation(Azurite.class).isEmpty()) {
      return false;
    }
    return parameterContext.getParameter().getType().isAssignableFrom(AzuriteAccess.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return extensionContext
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(
            AzuriteExtension.class.getName() + '#' + parameterContext.getParameter().getName(),
            k -> {
              Azurite azurite = parameterContext.findAnnotation(Azurite.class).get();
              return createContainer(azurite);
            },
            AzuriteAccess.class);
  }

  private AzuriteAccess createContainer(Azurite azurite) {
    String bucket = nonDefault(azurite.storageContainer());
    String account = nonDefault(azurite.storageContainer());
    String secret = nonDefault(azurite.storageContainer());
    AzuriteContainer container =
        new AzuriteContainer(null, bucket, account, secret).withStartupAttempts(5);
    container.start();
    return container;
  }

  private static String nonDefault(String s) {
    return s.equals(Azurite.DEFAULT) ? null : s;
  }
}

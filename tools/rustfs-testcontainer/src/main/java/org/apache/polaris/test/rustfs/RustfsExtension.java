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

package org.apache.polaris.test.rustfs;

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

/**
 * JUnit extension that provides a Rustfs container configured with a single bucket.
 *
 * <p>Provides instances of {@link RustfsAccess} via instance or static fields or parameters
 * annotated with {@link Rustfs}.
 */
// CODE_COPIED_TO_POLARIS from Project Nessie 0.104.2
public class RustfsExtension
    implements BeforeAllCallback, BeforeEachCallback, ParameterResolver, ExecutionCondition {
  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(RustfsExtension.class);

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    if (OS.current() == OS.LINUX) {
      return enabled("Running on Linux");
    }
    if (OS.current() == OS.MAC
        && System.getenv("CI_MAC") == null
        && RustfsContainer.canRunOnMacOs()) {
      // Disable tests on GitHub Actions
      return enabled("Running on macOS locally");
    }
    return disabled(format("Disabled on %s", OS.current().name()));
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();

    findAnnotatedFields(testClass, Rustfs.class, ReflectionUtils::isStatic)
        .forEach(field -> injectField(context, field));
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();

    findAnnotatedFields(testClass, Rustfs.class, ReflectionUtils::isNotStatic)
        .forEach(field -> injectField(context, field));
  }

  private void injectField(ExtensionContext context, Field field) {
    try {
      Rustfs rustfs =
          AnnotationUtils.findAnnotation(field, Rustfs.class)
              .orElseThrow(IllegalStateException::new);

      RustfsAccess container =
          context
              .getStore(NAMESPACE)
              .getOrComputeIfAbsent(
                  field.toString(), x -> createContainer(rustfs), RustfsAccess.class);

      makeAccessible(field).set(context.getTestInstance().orElse(null), container);
    } catch (Throwable t) {
      ExceptionUtils.throwAsUncheckedException(t);
    }
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    if (parameterContext.findAnnotation(Rustfs.class).isEmpty()) {
      return false;
    }
    return parameterContext.getParameter().getType().isAssignableFrom(RustfsAccess.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return extensionContext
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(
            RustfsExtension.class.getName() + '#' + parameterContext.getParameter().getName(),
            k -> {
              Rustfs rustfs = parameterContext.findAnnotation(Rustfs.class).get();
              return createContainer(rustfs);
            },
            RustfsAccess.class);
  }

  private RustfsAccess createContainer(Rustfs rustfs) {
    String accessKey = nonDefault(rustfs.accessKey());
    String secretKey = nonDefault(rustfs.secretKey());
    String bucket = nonDefault(rustfs.bucket());
    String region = nonDefault(rustfs.region());
    RustfsContainer container =
        new RustfsContainer(null, accessKey, secretKey, bucket, region).withStartupAttempts(5);
    container.start();
    return container;
  }

  private static String nonDefault(String s) {
    return s.equals(Rustfs.DEFAULT) ? null : s;
  }
}

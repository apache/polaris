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

package org.apache.polaris.test.minio;

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
 * JUnit extension that provides a Minio container configured with a single bucket.
 *
 * <p>Provides instances of {@link MinioAccess} via instance or static fields or parameters
 * annotated with {@link Minio}.
 */
// CODE_COPIED_TO_POLARIS from Project Nessie 0.104.2
public class MinioExtension
    implements BeforeAllCallback, BeforeEachCallback, ParameterResolver, ExecutionCondition {
  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(MinioExtension.class);

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    if (OS.current() == OS.LINUX) {
      return enabled("Running on Linux");
    }
    if (OS.current() == OS.MAC
        && System.getenv("CI_MAC") == null
        && MinioContainer.canRunOnMacOs()) {
      // Disable tests on GitHub Actions
      return enabled("Running on macOS locally");
    }
    return disabled(format("Disabled on %s", OS.current().name()));
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();

    findAnnotatedFields(testClass, Minio.class, ReflectionUtils::isStatic)
        .forEach(field -> injectField(context, field));
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();

    findAnnotatedFields(testClass, Minio.class, ReflectionUtils::isNotStatic)
        .forEach(field -> injectField(context, field));
  }

  private void injectField(ExtensionContext context, Field field) {
    try {
      Minio minio =
          AnnotationUtils.findAnnotation(field, Minio.class)
              .orElseThrow(IllegalStateException::new);

      MinioAccess container =
          context
              .getStore(NAMESPACE)
              .getOrComputeIfAbsent(
                  field.toString(), x -> createContainer(minio), MinioAccess.class);

      makeAccessible(field).set(context.getTestInstance().orElse(null), container);
    } catch (Throwable t) {
      ExceptionUtils.throwAsUncheckedException(t);
    }
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    if (parameterContext.findAnnotation(Minio.class).isEmpty()) {
      return false;
    }
    return parameterContext.getParameter().getType().isAssignableFrom(MinioAccess.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return extensionContext
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(
            MinioExtension.class.getName() + '#' + parameterContext.getParameter().getName(),
            k -> {
              Minio minio = parameterContext.findAnnotation(Minio.class).get();
              return createContainer(minio);
            },
            MinioAccess.class);
  }

  private MinioAccess createContainer(Minio minio) {
    String accessKey = nonDefault(minio.accessKey());
    String secretKey = nonDefault(minio.secretKey());
    String bucket = nonDefault(minio.bucket());
    MinioContainer container =
        new MinioContainer(null, accessKey, secretKey, bucket).withStartupAttempts(5);
    container.start();
    return container;
  }

  private static String nonDefault(String s) {
    return s.equals(Minio.DEFAULT) ? null : s;
  }
}

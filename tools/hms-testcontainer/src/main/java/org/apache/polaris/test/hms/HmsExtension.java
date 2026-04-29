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
package org.apache.polaris.test.hms;

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotatedFields;
import static org.junit.platform.commons.util.ReflectionUtils.makeAccessible;

import java.lang.reflect.Field;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;

/**
 * JUnit 5 extension that provides a single shared {@link HmsContainer} per test class. Provides
 * instances of {@link HmsAccess} via instance/static fields or method parameters annotated with
 * {@link Hms}.
 */
public class HmsExtension implements BeforeAllCallback, BeforeEachCallback, ParameterResolver {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(HmsExtension.class);
  private static final String CONTAINER_KEY = "hms-container";

  @Override
  public void beforeAll(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();
    findAnnotatedFields(testClass, Hms.class, ReflectionUtils::isStatic)
        .forEach(field -> injectField(context, field));
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();
    findAnnotatedFields(testClass, Hms.class, ReflectionUtils::isNotStatic)
        .forEach(field -> injectField(context, field));
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    if (parameterContext.findAnnotation(Hms.class).isEmpty()) {
      return false;
    }
    return parameterContext.getParameter().getType().isAssignableFrom(HmsAccess.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return getOrCreateContainer(extensionContext);
  }

  private void injectField(ExtensionContext context, Field field) {
    try {
      HmsAccess container = getOrCreateContainer(context);
      makeAccessible(field).set(context.getTestInstance().orElse(null), container);
    } catch (Throwable t) {
      ExceptionUtils.throwAsUncheckedException(t);
    }
  }

  private HmsAccess getOrCreateContainer(ExtensionContext context) {
    return context
        .getRoot()
        .getStore(NAMESPACE)
        .computeIfAbsent(
            CONTAINER_KEY,
            k -> {
              HmsContainer container = new HmsContainer().withStartupAttempts(3);
              container.start();
              return container;
            },
            HmsContainer.class);
  }
}

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

package org.apache.polaris.test.floci.aws;

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotatedFields;
import static org.junit.platform.commons.util.ReflectionUtils.makeAccessible;

import java.lang.reflect.Field;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.AnnotationUtils;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;

public class FlociAwsExtension implements BeforeAllCallback, BeforeEachCallback, ParameterResolver {
  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(FlociAwsExtension.class);

  @Override
  public void beforeAll(ExtensionContext context) {
    findAnnotatedFields(context.getRequiredTestClass(), FlociAws.class, ReflectionUtils::isStatic)
        .forEach(field -> injectField(context, field));
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    findAnnotatedFields(
            context.getRequiredTestClass(), FlociAws.class, ReflectionUtils::isNotStatic)
        .forEach(field -> injectField(context, field));
  }

  private void injectField(ExtensionContext context, Field field) {
    try {
      FlociAws flociAws =
          AnnotationUtils.findAnnotation(field, FlociAws.class)
              .orElseThrow(IllegalStateException::new);
      makeAccessible(field).set(context.getTestInstance().orElse(null), access(context, flociAws));
    } catch (Throwable t) {
      ExceptionUtils.throwAsUncheckedException(t);
    }
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.findAnnotation(FlociAws.class).isPresent()
        && parameterContext.getParameter().getType().isAssignableFrom(FlociAwsAccess.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    FlociAws flociAws = parameterContext.findAnnotation(FlociAws.class).get();
    return access(extensionContext, flociAws);
  }

  private FlociAwsAccess access(ExtensionContext context, FlociAws flociAws) {
    Config config = config(flociAws);
    StoredContainer stored =
        context
            .getRoot()
            .getStore(NAMESPACE)
            .computeIfAbsent(
                config,
                ignored -> new StoredContainer(createContainer(config)),
                StoredContainer.class);
    return stored.container();
  }

  private FlociAwsContainer createContainer(Config config) {
    FlociAwsContainer container =
        new FlociAwsContainer(
                null,
                config.accessKey(),
                config.secretKey(),
                config.bucket(),
                config.region(),
                config.iamEnforcement())
            .withStartupAttempts(5);
    container.start();
    return container;
  }

  private static Config config(FlociAws flociAws) {
    return new Config(
        nonDefault(flociAws.accessKey()),
        nonDefault(flociAws.secretKey()),
        nonDefault(flociAws.bucket()),
        nonDefault(flociAws.region()),
        flociAws.iamEnforcement());
  }

  private static String nonDefault(String s) {
    return s.equals(FlociAws.DEFAULT) ? null : s;
  }

  private record Config(
      String accessKey, String secretKey, String bucket, String region, boolean iamEnforcement) {}

  private record StoredContainer(FlociAwsContainer container) implements AutoCloseable {
    @Override
    public void close() {
      container.close();
    }
  }
}

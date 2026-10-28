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

package org.apache.polaris.test.floci.gcp;

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

public class FlociGcpExtension implements BeforeAllCallback, BeforeEachCallback, ParameterResolver {
  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(FlociGcpExtension.class);

  @Override
  public void beforeAll(ExtensionContext context) {
    findAnnotatedFields(context.getRequiredTestClass(), FlociGcp.class, ReflectionUtils::isStatic)
        .forEach(field -> injectField(context, field));
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    findAnnotatedFields(
            context.getRequiredTestClass(), FlociGcp.class, ReflectionUtils::isNotStatic)
        .forEach(field -> injectField(context, field));
  }

  private void injectField(ExtensionContext context, Field field) {
    try {
      FlociGcp flociGcp =
          AnnotationUtils.findAnnotation(field, FlociGcp.class)
              .orElseThrow(IllegalStateException::new);
      makeAccessible(field).set(context.getTestInstance().orElse(null), access(context, flociGcp));
    } catch (Throwable t) {
      ExceptionUtils.throwAsUncheckedException(t);
    }
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.findAnnotation(FlociGcp.class).isPresent()
        && parameterContext.getParameter().getType().isAssignableFrom(FlociGcpAccess.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return access(extensionContext, parameterContext.findAnnotation(FlociGcp.class).get());
  }

  private FlociGcpAccess access(ExtensionContext context, FlociGcp flociGcp) {
    Config config = config(flociGcp);
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

  private FlociGcpContainer createContainer(Config config) {
    FlociGcpContainer container =
        new FlociGcpContainer(null, config.bucket(), config.projectId(), config.oauth2Token())
            .withStartupAttempts(5);
    container.start();
    return container;
  }

  private static Config config(FlociGcp flociGcp) {
    return new Config(
        nonDefault(flociGcp.bucket()),
        nonDefault(flociGcp.projectId()),
        nonDefault(flociGcp.oauth2Token()));
  }

  private static String nonDefault(String s) {
    return s.equals(FlociGcp.DEFAULT) ? null : s;
  }

  private record Config(String bucket, String projectId, String oauth2Token) {}

  private record StoredContainer(FlociGcpContainer container) implements AutoCloseable {
    @Override
    public void close() {
      container.close();
    }
  }
}

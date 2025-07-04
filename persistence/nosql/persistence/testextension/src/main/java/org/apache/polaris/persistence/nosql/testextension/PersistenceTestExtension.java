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
package org.apache.polaris.persistence.nosql.testextension;

import static com.google.common.base.Preconditions.checkState;
import static java.util.function.Function.identity;
import static org.apache.polaris.persistence.nosql.testextension.PolarisPersistence.RANDOM_REALM;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotatedFields;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;
import static org.junit.platform.commons.util.ReflectionUtils.isPrivate;
import static org.junit.platform.commons.util.ReflectionUtils.makeAccessible;

import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.ids.api.SnowflakeIdGenerator;
import org.apache.polaris.ids.impl.MonotonicClockImpl;
import org.apache.polaris.ids.impl.SnowflakeIdGeneratorFactory;
import org.apache.polaris.ids.spi.IdGeneratorSource;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.CacheSizing;
import org.apache.polaris.persistence.nosql.api.commit.FairRetriesType;
import org.apache.polaris.persistence.nosql.api.commit.RetryConfig;
import org.apache.polaris.persistence.nosql.impl.cache.PersistenceCaches;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistenceTestExtension
    implements BeforeAllCallback, BeforeEachCallback, ParameterResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(PersistenceTestExtension.class);

  static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(PersistenceTestExtension.class);
  static final String KEY_BACKEND = "polaris-test-backend";
  static final String KEY_BACKEND_TEST_FACTORY = "polaris-test-backend-test-factory";
  static final String KEY_MONOTONIC_CLOCK = "polaris-monotonic-clock";
  static final String KEY_SNOWFLAKE_ID_GENERATOR = "polaris-snowflake-id-generator";

  @Override
  public void beforeAll(ExtensionContext extensionContext) {
    var testClass = extensionContext.getRequiredTestClass();

    findAnnotatedFields(testClass, PolarisPersistence.class, ReflectionUtils::isStatic)
        .forEach(field -> injectField(extensionContext, null, field));
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) {
    extensionContext
        .getRequiredTestInstances()
        .getAllInstances() //
        .forEach(
            instance ->
                findAnnotatedFields(
                        instance.getClass(), PolarisPersistence.class, ReflectionUtils::isNotStatic)
                    .forEach(field -> injectField(extensionContext, instance, field)));
  }

  private void injectField(ExtensionContext extensionContext, Object instance, Field field) {
    assertValidFieldCandidate(field);
    try {
      PolarisPersistence annotation =
          findAnnotation(field, PolarisPersistence.class).orElseThrow(IllegalStateException::new);

      Object assign = resolve(annotation, field.getType(), extensionContext);

      makeAccessible(field).set(instance, assign);
    } catch (Throwable t) {
      ExceptionUtils.throwAsUncheckedException(t);
    }
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    PolarisPersistence annotation =
        parameterContext
            .findAnnotation(PolarisPersistence.class)
            .orElseThrow(IllegalStateException::new);
    Parameter parameter = parameterContext.getParameter();

    return resolve(annotation, parameter.getType(), extensionContext);
  }

  private Object resolve(
      PolarisPersistence annotation, Class<?> type, ExtensionContext extensionContext) {

    if (MonotonicClock.class.isAssignableFrom(type)) {
      return getOrCreateMonotonicClock(extensionContext);
    }
    if (IdGenerator.class.isAssignableFrom(type)) {
      return getOrCreateSnowflakeIdGenerator(extensionContext);
    }

    BackendSpec backendSpec = findBackendSpec(extensionContext);
    checkState(
        backendSpec != null,
        "Cannot find backend spec for %s",
        extensionContext.getRequiredTestClass());

    if (BackendTestFactory.class.isAssignableFrom(type)) {
      return getOrCreateBackendTestFactory(extensionContext, backendSpec);
    }
    if (Backend.class.isAssignableFrom(type)) {
      return getOrCreateBackend(extensionContext, backendSpec);
    }
    if (Persistence.class.isAssignableFrom(type)) {
      return createPersistence(annotation, extensionContext, backendSpec);
    }

    throw new IllegalStateException("Unable to assign a field of type " + type);
  }

  private BackendSpec findBackendSpec(ExtensionContext extensionContext) {
    for (; extensionContext != null; extensionContext = extensionContext.getParent().orElse(null)) {
      var maybe =
          extensionContext.getTestClass().flatMap(c -> findAnnotation(c, BackendSpec.class, true));
      if (maybe.isPresent()) {
        return maybe.get();
      }
    }
    return null;
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.isAnnotated(PolarisPersistence.class);
  }

  private void assertValidFieldCandidate(Field field) {
    if (!field.getType().isAssignableFrom(SnowflakeIdGenerator.class)
        && !field.getType().isAssignableFrom(MonotonicClock.class)
        && !field.getType().isAssignableFrom(Persistence.class)
        && !field.getType().isAssignableFrom(Backend.class)
        && !field.getType().isAssignableFrom(BackendTestFactory.class)) {
      throw new ExtensionConfigurationException(
          "Unsupported field type " + field.getType().getName());
    }
    if (isPrivate(field)) {
      throw new ExtensionConfigurationException(
          String.format("field [%s] must not be private.", field));
    }
  }

  private MonotonicClock getOrCreateMonotonicClock(ExtensionContext extensionContext) {
    var store = extensionContext.getRoot().getStore(NAMESPACE);
    return store
        .getOrComputeIfAbsent(
            KEY_MONOTONIC_CLOCK,
            x -> new WrappedResource(MonotonicClockImpl.newDefaultInstance()),
            WrappedResource.class)
        .resource();
  }

  private SnowflakeIdGenerator getOrCreateSnowflakeIdGenerator(ExtensionContext extensionContext) {
    var store = extensionContext.getRoot().getStore(NAMESPACE);
    return store.getOrComputeIfAbsent(
        KEY_SNOWFLAKE_ID_GENERATOR,
        x ->
            new SnowflakeIdGeneratorFactory()
                .buildIdGenerator(
                    Map.of(),
                    new IdGeneratorSource() {
                      final MonotonicClock clock = getOrCreateMonotonicClock(extensionContext);

                      @Override
                      public int nodeId() {
                        return 123;
                      }

                      @Override
                      public long currentTimeMillis() {
                        return clock.currentTimeMillis();
                      }
                    }),
        SnowflakeIdGenerator.class);
  }

  private BackendTestFactory getOrCreateBackendTestFactory(
      ExtensionContext extensionContext, BackendSpec backendSpec) {
    var store = extensionContext.getRoot().getStore(NAMESPACE);
    var existingResource = store.get(KEY_BACKEND, WrappedResource.class);
    var existing =
        existingResource != null ? existingResource.<BackendTestFactory>resource() : null;
    if (existing != null) {
      if (isCompatible(backendSpec, existing)) {
        return existing;
      }
      var previous = store.remove(KEY_BACKEND_TEST_FACTORY, WrappedResource.class);
      LOGGER.info(
          "Stopping previously used persistence backend test factory '{}' because it is incompatible with {}",
          existing.name(),
          backendSpec);
      try {
        previous.resource.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    LOGGER.info("Creating new persistence backend for {}", backendSpec);
    var factory =
        BackendTestFactoryLoader.findFactory(
            f -> {
              if (!backendSpec.name().isEmpty() && !backendSpec.name().equalsIgnoreCase(f.name())) {
                return false;
              }
              return backendSpec.factory() == BackendTestFactory.class
                  || backendSpec.factory().isInstance(f);
            });
    try {
      factory.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    store.put(KEY_BACKEND_TEST_FACTORY, new WrappedResource(factory));
    return factory;
  }

  private Backend getOrCreateBackend(ExtensionContext extensionContext, BackendSpec backendSpec) {
    var store = extensionContext.getRoot().getStore(NAMESPACE);
    var existingResource = store.get(KEY_BACKEND, WrappedResource.class);
    var existing = existingResource != null ? existingResource.<Backend>resource() : null;
    if (existing != null) {
      var existingFactory =
          store.get(KEY_BACKEND_TEST_FACTORY, WrappedResource.class).<BackendTestFactory>resource();
      if (isCompatible(backendSpec, existingFactory)) {
        return existing;
      }
      try {
        var previous = store.remove(KEY_BACKEND, WrappedResource.class);
        var backend = previous.<Backend>resource();
        LOGGER.info(
            "Closing previously used persistence backend '{}' because it is incompatible with {}",
            backend.type(),
            backendSpec);
        backend.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    var testFactory = getOrCreateBackendTestFactory(extensionContext, backendSpec);
    try {
      var instance = testFactory.createNewBackend();
      var info = instance.setupSchema().orElse("");
      LOGGER.info("Opened new persistence backend '{}' {}", instance.type(), info);
      store.put(KEY_BACKEND, new WrappedResource(instance));
      return instance;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isCompatible(BackendSpec backendSpec, BackendTestFactory existingFactory) {
    if (backendSpec.factory() != BackendTestFactory.class
        && !backendSpec.factory().isAssignableFrom(existingFactory.getClass())) {
      return false;
    }
    if (!backendSpec.name().isEmpty()) {
      return backendSpec.name().equals(existingFactory.name());
    }
    return true;
  }

  private Persistence createPersistence(
      PolarisPersistence annotation, ExtensionContext extensionContext, BackendSpec backendSpec) {
    var clock = getOrCreateMonotonicClock(extensionContext);
    var idGenerator = getOrCreateSnowflakeIdGenerator(extensionContext);

    var backend = getOrCreateBackend(extensionContext, backendSpec);
    var realmId =
        RANDOM_REALM.equals(annotation.realmId())
            ? UUID.randomUUID().toString()
            : annotation.realmId();
    var persistenceConfig = PersistenceParams.BuildablePersistenceParams.builder();
    if (annotation.fastRetries()) {
      persistenceConfig.retryConfig(
          RetryConfig.BuildableRetryConfig.builder()
              .initialSleepLower(Duration.ZERO)
              .maxSleep(Duration.ofMillis(1))
              .initialSleepUpper(Duration.ofMillis(1))
              .timeout(Duration.ofMinutes(5))
              .retries(Integer.MAX_VALUE)
              .fairRetries(FairRetriesType.UNFAIR)
              .build());
    }
    var uncachedPersistence =
        backend.newPersistence(identity(), persistenceConfig.build(), realmId, clock, idGenerator);

    if (!annotation.caching()) {
      return uncachedPersistence;
    }

    return PersistenceCaches.newBackend(
            CacheConfig.BuildableCacheConfig.builder()
                .sizing(CacheSizing.builder().fixedSize(MemorySize.ofMega(32)).build())
                .clockNanos(clock::nanoTime)
                .referenceTtl(Duration.ofMinutes(1))
                .referenceNegativeTtl(Duration.ofSeconds(1))
                .build(),
            Optional.empty())
        .wrap(uncachedPersistence);
  }

  static final class WrappedResource implements AutoCloseable {
    final AutoCloseable resource;

    WrappedResource(AutoCloseable resource) {
      this.resource = resource;
    }

    @Override
    public void close() throws Exception {
      resource.close();
    }

    @SuppressWarnings("unchecked")
    <X> X resource() {
      return (X) resource;
    }
  }
}

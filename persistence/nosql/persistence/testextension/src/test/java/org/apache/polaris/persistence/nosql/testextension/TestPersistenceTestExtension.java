/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may obtain a copy of the License at
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.ids.api.SnowflakeIdGenerator;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestPersistenceTestExtension {
  private final PersistenceTestExtension extension = new PersistenceTestExtension();

  @ParameterizedTest
  @ValueSource(
      strings = {
        "monotonicClock",
        "idGenerator",
        "snowflakeIdGenerator",
        "backendTestFactory",
        "backend",
        "persistence"
      })
  public void acceptsCompatibleSubtypeFields(String fieldName) throws Exception {
    var field = FieldFixtures.class.getDeclaredField(fieldName);
    assertThat(PersistenceTestExtension.isValidFieldCandidate(field.getType())).isTrue();
    assertThatCode(() -> extension.assertValidFieldCandidate(field)).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(strings = {"unsupported", "unsupportedDummyBackendTestFactory"})
  public void rejectsUnsupportedSupertypes(String fieldName) throws Exception {
    var field = FieldFixtures.class.getDeclaredField(fieldName);

    assertThatThrownBy(() -> extension.assertValidFieldCandidate(field))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageStartingWith("Unsupported field type ");
  }

  @Test
  public void backendTestFactoryUsesDedicatedStoreKey() throws Throwable {
    var store = new MapStore();
    var context = extensionContext(store.store());
    var backendSpec = BackendSpecCarrier.class.getAnnotation(BackendSpec.class);

    var backend =
        (Backend)
            Proxy.newProxyInstance(
                Backend.class.getClassLoader(),
                new Class<?>[] {Backend.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("close")) {
                    return null;
                  }
                  if (method.getName().equals("type")) {
                    return "dummy";
                  }
                  throw new UnsupportedOperationException(method.getName());
                });
    var existingFactory = new DummyBackendTestFactory("dummy");

    store.put(
        PersistenceTestExtension.KEY_BACKEND,
        new PersistenceTestExtension.WrappedResource(backend));
    store.put(
        PersistenceTestExtension.KEY_BACKEND_TEST_FACTORY,
        new PersistenceTestExtension.WrappedResource(existingFactory));

    var resolvedFactory = extension.getOrCreateBackendTestFactory(context, backendSpec);

    assertThat(resolvedFactory).isSameAs(existingFactory);
  }

  private ExtensionContext extensionContext(ExtensionContext.Store store) {
    return (ExtensionContext)
        Proxy.newProxyInstance(
            ExtensionContext.class.getClassLoader(),
            new Class<?>[] {ExtensionContext.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "getRoot" -> proxy;
                case "getStore" -> store;
                case "getParent" -> Optional.empty();
                case "getTestClass" -> Optional.of(BackendSpecCarrier.class);
                case "getRequiredTestClass" -> BackendSpecCarrier.class;
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }

  static final class MapStore {
    private final Map<Object, Object> values = new HashMap<>();
    private final ExtensionContext.Store store =
        (ExtensionContext.Store)
            Proxy.newProxyInstance(
                ExtensionContext.Store.class.getClassLoader(),
                new Class<?>[] {ExtensionContext.Store.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "get" -> values.get(args[0]);
                    case "put" -> {
                      values.put(args[0], args[1]);
                      yield null;
                    }
                    case "remove" -> values.remove(args[0]);
                    default -> throw new UnsupportedOperationException(method.getName());
                  };
                });

    ExtensionContext.Store store() {
      return store;
    }

    public void put(Object key, Object value) {
      values.put(key, value);
    }
  }

  static final class DummyBackendTestFactory implements BackendTestFactory {
    private final String name;

    DummyBackendTestFactory(String name) {
      this.name = name;
    }

    @Override
    public Backend createNewBackend() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public String name() {
      return name;
    }
  }

  @BackendSpec(name = "dummy")
  static class BackendSpecCarrier {}

  @SuppressWarnings("unused")
  static class FieldFixtures {
    @PolarisPersistence MonotonicClock monotonicClock;
    @PolarisPersistence IdGenerator idGenerator;
    @PolarisPersistence SnowflakeIdGenerator snowflakeIdGenerator;
    @PolarisPersistence BackendTestFactory backendTestFactory;
    @PolarisPersistence Backend backend;
    @PolarisPersistence Persistence persistence;
    @PolarisPersistence DummyBackendTestFactory unsupportedDummyBackendTestFactory;
    @PolarisPersistence Object unsupported;
  }
}

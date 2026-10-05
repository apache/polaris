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
package org.apache.polaris.persistence.atomic.dynamodb;

import static org.apache.polaris.core.auth.AuthBootstrapUtil.createPolarisPrincipalForRealm;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.AtomicOperationMetaStoreManager;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.cache.InMemoryEntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * DynamoDB (Atomic) implementation of {@link MetaStoreManagerFactory}. Mirrors {@code
 * JdbcMetaStoreManagerFactory}: it returns the <em>reused</em> {@link
 * AtomicOperationMetaStoreManager} and {@link InMemoryEntityCache}, and the one new adapter {@link
 * DynamoDbBasePersistence}. Selected at startup with {@code
 * polaris.persistence.type=dynamodb-atomic}.
 *
 * <p>One shared table holds every realm ({@code realmId} is only a key prefix), so a single {@link
 * DynamoDbOperations} is reused across realms; the per-realm adapter just carries the realm id.
 */
@ApplicationScoped
@Identifier("dynamodb-atomic")
public class DynamoDbMetaStoreManagerFactory implements MetaStoreManagerFactory {

  private static final String DEFAULT_TABLE_NAME = "polaris_entities";

  private final Map<String, EntityCache> entityCacheMap = new ConcurrentHashMap<>();

  @Inject Clock clock;
  @Inject PolarisDiagnostics diagnostics;
  @Inject DynamoDbClient dynamoDbClient;
  @Inject DynamoDbConfiguration configuration;

  private volatile DynamoDbOperations operations;

  protected DynamoDbMetaStoreManagerFactory() {}

  /** Lazily build the shared operations wrapper and ensure the table exists (idempotent). */
  private DynamoDbOperations operations() {
    DynamoDbOperations ops = operations;
    if (ops == null) {
      synchronized (this) {
        ops = operations;
        if (ops == null) {
          ops =
              new DynamoDbOperations(
                  dynamoDbClient, configuration.tableName().orElse(DEFAULT_TABLE_NAME));
          ops.setupSchema();
          operations = ops;
        }
      }
    }
    return ops;
  }

  private PolarisMetaStoreManager createNewMetaStoreManager() {
    return new AtomicOperationMetaStoreManager(clock, diagnostics);
  }

  protected PrincipalSecretsGenerator secretsGenerator(
      String realmId, RootCredentialsSet rootCredentialsSet) {
    if (rootCredentialsSet != null) {
      return PrincipalSecretsGenerator.bootstrap(realmId, rootCredentialsSet);
    }
    return PrincipalSecretsGenerator.RANDOM_SECRETS;
  }

  /** Build a per-realm persistence session, with a secrets generator appropriate to the context. */
  private DynamoDbBasePersistence createSession(
      String realmId, RootCredentialsSet rootCredentialsSet) {
    return new DynamoDbBasePersistence(
        operations(), realmId, diagnostics, secretsGenerator(realmId, rootCredentialsSet));
  }

  @Override
  public PolarisMetaStoreManager getOrCreateMetaStoreManager(RealmContext realmContext) {
    return createNewMetaStoreManager();
  }

  @Override
  public BasePersistence getOrCreateSession(RealmContext realmContext) {
    return createSession(realmContext.getRealmIdentifier(), null);
  }

  @Override
  public EntityCache getOrCreateEntityCache(RealmContext realmContext, RealmConfig realmConfig) {
    return entityCacheMap.computeIfAbsent(
        realmContext.getRealmIdentifier(),
        k -> new InMemoryEntityCache(diagnostics, realmConfig, createNewMetaStoreManager()));
  }

  @Override
  public synchronized Map<String, PrincipalSecretsResult> bootstrapRealms(
      Iterable<String> realms, RootCredentialsSet rootCredentialsSet) {
    operations().setupSchema();
    Map<String, PrincipalSecretsResult> results = new HashMap<>();
    for (String realm : realms) {
      RealmContext realmContext = () -> realm;
      PolarisMetaStoreManager metaStoreManager = createNewMetaStoreManager();
      BasePersistence session = createSession(realm, rootCredentialsSet);
      PolarisCallContext callContext = new PolarisCallContext(realmContext, session);
      results.put(realm, createPolarisPrincipalForRealm(metaStoreManager, callContext));
    }
    return Map.copyOf(results);
  }

  @Override
  public synchronized Map<String, BaseResult> purgeRealms(Iterable<String> realms) {
    Map<String, BaseResult> results = new HashMap<>();
    for (String realm : realms) {
      RealmContext realmContext = () -> realm;
      PolarisMetaStoreManager metaStoreManager = createNewMetaStoreManager();
      BasePersistence session = getOrCreateSession(realmContext);
      PolarisCallContext callContext = new PolarisCallContext(realmContext, session);
      results.put(realm, metaStoreManager.purge(callContext));
      entityCacheMap.remove(realm);
    }
    return Map.copyOf(results);
  }
}

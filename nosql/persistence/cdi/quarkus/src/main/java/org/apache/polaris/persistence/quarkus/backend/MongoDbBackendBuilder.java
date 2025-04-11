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
package org.apache.polaris.persistence.quarkus.backend;

import com.mongodb.client.MongoClient;
import io.quarkus.arc.Arc;
import io.quarkus.mongodb.runtime.MongoClientBeanUtil;
import io.quarkus.mongodb.runtime.MongoClients;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.apache.polaris.persistence.api.backend.Backend;
import org.apache.polaris.persistence.mongodb.MongoDbBackendConfig;
import org.apache.polaris.persistence.mongodb.MongoDbBackendFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@BackendType(MongoDbBackendFactory.NAME)
@Dependent
class MongoDbBackendBuilder implements BackendBuilder {
  @Inject
  @ConfigProperty(name = "quarkus.mongodb.database", defaultValue = "polaris")
  String databaseName;

  @Override
  public Backend buildBackend() {
    MongoClients mongoClients = Arc.container().instance(MongoClients.class).get();
    MongoClient client =
        mongoClients.createMongoClient(MongoClientBeanUtil.DEFAULT_MONGOCLIENT_NAME);

    var config = new MongoDbBackendConfig(databaseName, client, true);

    return new MongoDbBackendFactory().buildBackend(config);
  }
}

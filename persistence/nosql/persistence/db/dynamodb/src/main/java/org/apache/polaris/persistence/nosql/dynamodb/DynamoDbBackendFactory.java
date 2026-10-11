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
package org.apache.polaris.persistence.nosql.dynamodb;

import jakarta.annotation.Nonnull;
import java.net.URI;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.BackendFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDbBackendFactory
    implements BackendFactory<DynamoDbBackendConfig, DynamoDbConfiguration> {
  public static final String NAME = "DynamoDb";

  @Override
  @Nonnull
  public String name() {
    return NAME;
  }

  @Override
  public DynamoDbBackendConfig buildConfiguration(DynamoDbConfiguration config) {
    var clientBuilder =
        DynamoDbClient.builder()
            .httpClientBuilder(Apache5HttpClient.builder())
            .region(Region.of(config.region().orElse("us-east-1")));

    // TODO credentials !
    clientBuilder.credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("xxx", "xxx")));

    config.endpointURI().ifPresent(uri -> clientBuilder.endpointOverride(URI.create(uri)));

    var client = clientBuilder.build();

    return new DynamoDbBackendConfig(true, client, config.tablePrefix());
  }

  @Override
  public Class<DynamoDbConfiguration> configurationInterface() {
    return DynamoDbConfiguration.class;
  }

  @Nonnull
  @Override
  public Backend buildBackend(@Nonnull DynamoDbBackendConfig backendConfig) {
    return new DynamoDbBackend(backendConfig);
  }
}

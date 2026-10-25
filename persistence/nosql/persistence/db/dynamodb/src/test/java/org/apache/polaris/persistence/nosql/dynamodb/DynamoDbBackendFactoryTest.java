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

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

class DynamoDbBackendFactoryTest {

  @Test
  void awsEndpointUsesDefaultCredentialsProvider() {
    var config = ImmutableBuildableDynamoDbConfiguration.builder().build();

    try (var client = new DynamoDbBackendFactory().buildConfiguration(config).client()) {
      assertThat(client.serviceClientConfiguration().credentialsProvider())
          .isInstanceOf(DefaultCredentialsProvider.class);
    }
  }

  @Test
  void endpointOverrideUsesDefaultCredentialsProvider() {
    var config =
        ImmutableBuildableDynamoDbConfiguration.builder()
            .endpointURI("http://localhost:8000")
            .build();

    try (var client = new DynamoDbBackendFactory().buildConfiguration(config).client()) {
      assertThat(client.serviceClientConfiguration().endpointOverride())
          .contains(URI.create("http://localhost:8000"));
      assertThat(client.serviceClientConfiguration().credentialsProvider())
          .isInstanceOf(DefaultCredentialsProvider.class);
    }
  }
}

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

import java.util.UUID;
import org.apache.polaris.test.floci.aws.FlociAws;
import org.apache.polaris.test.floci.aws.FlociAwsAccess;
import org.apache.polaris.test.floci.aws.FlociAwsExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Base for tests that exercise the backend against a real DynamoDB API served by a Floci container.
 * The container is injected and shared JVM-wide by {@link FlociAwsExtension} (which starts it once,
 * with startup retries, through the JUnit root store); each test uses its own uniquely named table
 * so tests stay isolated without restarting the container.
 */
@ExtendWith(FlociAwsExtension.class)
abstract class AbstractDynamoDbTest {

  @FlociAws(region = "us-east-1")
  protected static FlociAwsAccess flociAws;

  protected static DynamoDbClient client;

  @BeforeAll
  static void buildClient() {
    client =
        DynamoDbClient.builder()
            .endpointOverride(flociAws.endpoint())
            .credentialsProvider(flociAws.credentialsProvider())
            .region(Region.of(flociAws.region().orElse("us-east-1")))
            .build();
  }

  @AfterAll
  static void closeClient() {
    if (client != null) {
      client.close();
    }
  }

  protected static String uniqueTableName() {
    return "polaris_test_" + UUID.randomUUID().toString().replace("-", "");
  }
}

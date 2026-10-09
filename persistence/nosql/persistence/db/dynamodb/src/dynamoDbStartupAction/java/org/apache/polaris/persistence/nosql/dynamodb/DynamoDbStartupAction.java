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

import java.util.Map;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupAction;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupContext;
import org.apache.polaris.test.floci.aws.FlociAwsContainer;

public class DynamoDbStartupAction implements PolarisServerStartupAction {
  private FlociAwsContainer container;

  @Override
  public void start(PolarisServerStartupContext context) {
    container = new FlociAwsContainer();
    container.start();

    context
        .getSystemProperties()
        .putAll(
            Map.ofEntries(
                Map.entry("polaris.persistence.type", "nosql"),
                Map.entry("polaris.persistence.auto-bootstrap-types", "nosql"),
                Map.entry("polaris.persistence.nosql.backend", "DynamoDb"),
                Map.entry("quarkus.dynamodb.endpoint-override", container.endpoint().toString()),
                Map.entry("quarkus.dynamodb.aws.region", container.region().orElseThrow()),
                Map.entry("quarkus.dynamodb.aws.credentials.type", "static"),
                Map.entry(
                    "quarkus.dynamodb.aws.credentials.static-provider.access-key-id",
                    container.accessKey()),
                Map.entry(
                    "quarkus.dynamodb.aws.credentials.static-provider.secret-access-key",
                    container.secretKey())));
  }

  @Override
  public void close() {
    if (container != null) {
      try {
        container.close();
      } finally {
        container = null;
      }
    }
  }
}

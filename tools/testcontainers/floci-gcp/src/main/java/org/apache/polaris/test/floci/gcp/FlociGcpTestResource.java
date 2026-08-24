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

import static java.util.Objects.requireNonNull;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;

public class FlociGcpTestResource implements QuarkusTestResourceLifecycleManager {
  private Map<String, String> initArgs = Map.of();
  private FlociGcpContainer container;

  @Override
  public void init(Map<String, String> initArgs) {
    this.initArgs = Map.copyOf(initArgs);
  }

  @Override
  public Map<String, String> start() {
    this.container =
        new FlociGcpContainer(
                null,
                initArgs.get("bucket"),
                initArgs.get("projectId"),
                initArgs.get("oauth2Token"))
            .withStartupAttempts(5);
    this.container.start();
    return Map.of();
  }

  @Override
  public void stop() {
    if (container != null) {
      container.close();
    }
  }

  @Override
  public void inject(TestInjector testInjector) {
    FlociGcpAccess access = requireNonNull(container, "Floci GCP not started");
    testInjector.injectIntoFields(access, field -> field.isAnnotationPresent(FlociGcp.class));
  }
}

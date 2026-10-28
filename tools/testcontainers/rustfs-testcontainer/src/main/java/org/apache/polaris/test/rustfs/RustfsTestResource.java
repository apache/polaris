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

package org.apache.polaris.test.rustfs;

import static java.util.Objects.requireNonNull;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;

public class RustfsTestResource implements QuarkusTestResourceLifecycleManager {

  private Map<String, String> initArgs = Map.of();
  private RustfsContainer container;

  @Override
  public void init(Map<String, String> initArgs) {
    this.initArgs = Map.copyOf(initArgs);
  }

  @Override
  public Map<String, String> start() {
    var accessKey = initArgs.get("accessKey");
    var secretKey = initArgs.get("secretKey");
    var bucket = initArgs.get("bucket");
    var region = initArgs.get("region");
    this.container =
        new RustfsContainer(null, accessKey, secretKey, bucket, region).withStartupAttempts(5);
    this.container.start();
    return Map.of();
  }

  @Override
  public void stop() {
    if (container != null) {
      try {
        container.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void inject(TestInjector testInjector) {
    RustfsAccess rustfsAccess = requireNonNull(container, "RustFS not started");
    testInjector.injectIntoFields(rustfsAccess, field -> field.isAnnotationPresent(Rustfs.class));
  }
}

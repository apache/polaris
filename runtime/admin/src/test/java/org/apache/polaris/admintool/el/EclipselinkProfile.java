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
package org.apache.polaris.admintool.el;

import static org.apache.polaris.admintool.PostgresTestResourceLifecycleManager.INIT_SCRIPT;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.List;
import java.util.Map;
import org.apache.polaris.admintool.PostgresTestResourceLifecycleManager;

public class EclipselinkProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of();
  }

  @Override
  public List<TestResourceEntry> testResources() {
    return List.of(
        new TestResourceEntry(
            PostgresTestResourceLifecycleManager.class,
            Map.of(INIT_SCRIPT, "org/apache/polaris/admintool/init.sql")));
  }
}

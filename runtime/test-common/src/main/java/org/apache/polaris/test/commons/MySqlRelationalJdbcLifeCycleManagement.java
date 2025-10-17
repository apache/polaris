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
package org.apache.polaris.test.commons;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;

public class MySqlRelationalJdbcLifeCycleManagement
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

  public static final String INIT_SCRIPT = RelationalJdbcLifeCycleManagement.INIT_SCRIPT;

  private final RelationalJdbcLifeCycleManagement delegate =
      new RelationalJdbcLifeCycleManagement();

  @Override
  public void init(Map<String, String> initArgs) {
    Map<String, String> enhancedArgs = new java.util.HashMap<>(initArgs);
    enhancedArgs.put(RelationalJdbcLifeCycleManagement.DATABASE_TYPE, "MYSQL");
    delegate.init(enhancedArgs);
  }

  @Override
  public Map<String, String> start() {
    return delegate.start();
  }

  @Override
  public void stop() {
    delegate.stop();
  }

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    delegate.setIntegrationTestContext(context);
  }
}

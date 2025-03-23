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
package org.apache.polaris.service.quarkus.it;

import io.quarkus.test.junit.callback.QuarkusTestBeforeClassCallback;
import org.assertj.core.api.Assumptions;
import org.assertj.core.configuration.PreferredAssumptionException;

public class PolarisQuarkusTestBeforeClassCallback implements QuarkusTestBeforeClassCallback {
  @Override
  public void beforeClass(Class<?> testClass) {
    // Set preferredAssumptionException as Quarkus does not suppress JUnit4's
    // AssumptionViolatedException
    Assumptions.setPreferredAssumptionException(PreferredAssumptionException.JUNIT5);
  }
}

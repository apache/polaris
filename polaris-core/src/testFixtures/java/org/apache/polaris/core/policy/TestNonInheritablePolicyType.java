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
package org.apache.polaris.core.policy;

/**
 * A test-only non-inheritable policy type for use in integration tests.
 *
 * <p>This policy type is used to test the non-inheritable policy behavior in PolicyCatalog. It must
 * be registered via {@link PolicyType#registerCustomPolicyType(PolicyType)} before use and
 * unregistered in test cleanup.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * @BeforeEach
 * void setUp() {
 *   PolicyType.registerCustomPolicyType(TestNonInheritablePolicyType.INSTANCE);
 * }
 *
 * @AfterEach
 * void tearDown() {
 *   PolicyType.unregisterCustomPolicyType(TestNonInheritablePolicyType.INSTANCE);
 * }
 * }</pre>
 */
public enum TestNonInheritablePolicyType implements PolicyType {
  INSTANCE(1000, "test.non-inheritable");

  private final int code;
  private final String name;

  TestNonInheritablePolicyType(int code, String name) {
    this.code = code;
    this.name = name;
  }

  @Override
  public int getCode() {
    return code;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isInheritable() {
    return false;
  }
}

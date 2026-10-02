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
package org.apache.polaris.test.opa;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;

/**
 * A {@link QuarkusTestResourceLifecycleManager} that starts an <a
 * href="https://www.openpolicyagent.org/">Open Policy Agent</a> (OPA) container, uploads a Rego
 * policy, and points Polaris at it by returning {@code polaris.authorization.type=opa} and {@code
 * polaris.authorization.opa.policy-uri}.
 *
 * <p>The Rego policy can be supplied via the {@value #REGO_POLICY_ARG} init argument; otherwise a
 * default policy that denies everything is used. The policy must declare {@code package
 * polaris.authz}.
 */
public class OpaTestResource implements QuarkusTestResourceLifecycleManager {

  /**
   * Init argument holding the Rego policy to upload. Must declare {@code package polaris.authz}.
   */
  public static final String REGO_POLICY_ARG = "rego-policy";

  private static final String POLICY_NAME = "polaris/authz";

  private static final String DEFAULT_POLICY =
      """
      package polaris.authz

      default allow := false
      """;

  private OpaContainer opa;
  private String regoPolicy;

  @Override
  public void init(Map<String, String> initArgs) {
    regoPolicy = initArgs.getOrDefault(REGO_POLICY_ARG, DEFAULT_POLICY);
  }

  @Override
  public Map<String, String> start() {
    opa = new OpaContainer();
    opa.start();
    opa.uploadRegoPolicy(POLICY_NAME, regoPolicy);
    return Map.of(
        "polaris.authorization.type",
        "opa",
        "polaris.authorization.opa.policy-uri",
        opa.getExternalUrl() + "v1/data/" + POLICY_NAME);
  }

  @Override
  public void stop() {
    if (opa != null) {
      try {
        opa.stop();
      } finally {
        opa = null;
      }
    }
  }
}

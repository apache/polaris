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
package org.apache.polaris.extension.auth.opa.test;

import org.apache.polaris.server.test.runner.spi.PolarisServerStartupAction;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupContext;
import org.apache.polaris.test.opa.OpaContainer;

/** Starts an OPA test server before the external Polaris server process starts. */
public class OpaStartupAction implements PolarisServerStartupAction {

  private static final String POLICY_NAME = "polaris/authz";
  private static final String POLICY_PACKAGE = POLICY_NAME.replace('/', '.');

  private static final String POLICY =
      """
        package %s

        default allow := false

        # Allow root user for all operations
        allow if {
          input.actor.principal == "root"
        }

        # Allow admin user for all operations
        allow if {
          input.actor.principal == "admin"
        }
        """
          .formatted(POLICY_PACKAGE);

  private OpaContainer opa;

  @Override
  public void start(PolarisServerStartupContext context) {
    opa = new OpaContainer();
    opa.start();
    opa.uploadRegoPolicy(POLICY_NAME, POLICY);
    context
        .getSystemProperties()
        .put(
            "polaris.authorization.opa.policy-uri",
            opa.getExternalUrl() + "v1/data/" + POLICY_NAME);
  }

  @Override
  public void close() {
    if (opa != null) {
      opa.stop();
      opa = null;
    }
  }
}

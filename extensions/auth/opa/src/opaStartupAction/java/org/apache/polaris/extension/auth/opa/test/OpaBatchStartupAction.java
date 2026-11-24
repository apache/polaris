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

/**
 * Starts an OPA test server with a batch rule before the external Polaris server process starts,
 * and configures Polaris to use it with a small {@code max-batch-size}, so that a list of a few
 * entities is already split into several chunks.
 */
public class OpaBatchStartupAction implements PolarisServerStartupAction {

  private static final String POLICY_NAME = "polaris/authz";
  private static final String POLICY_PACKAGE = POLICY_NAME.replace('/', '.');

  /** Small on purpose: the tests list more entities than this. */
  private static final String MAX_BATCH_SIZE = "2";

  private static final String POLICY =
      """
      package %s

      default allow := false

      allow if {
        input.actor.principal == "root"
      }

      allow if {
        input.actor.principal == "admin"
      }

      # A principal named "lister-*" may see only the entities named "visible_*".
      allow if {
        startswith(input.actor.principal, "lister-")
        input.action in {"LIST_NAMESPACE", "LIST_TABLE", "LIST_VIEW"}
        some target in input.resource.targets
        startswith(target.name, "visible_")
      }

      # The table "batch_only_table" is visible only through the batch rule. The single-question
      # rule above never allows it, so seeing it in a list proves the batch rule answered.
      batch_only(r) if {
        some target in r.resource.targets
        target.name == "batch_only_table"
      }

      batch_allow[r.id] := true if {
        some r in input.requests
        batch_only(r)
      }

      # Every other question gets the same answer as the single-question rule.
      batch_allow[r.id] := decision if {
        some r in input.requests
        not batch_only(r)
        decision := allow with input as {
          "actor": input.actor,
          "context": input.context,
          "action": r.action,
          "resource": r.resource,
        }
      }
      """
          .formatted(POLICY_PACKAGE);

  private OpaContainer opa;

  @Override
  public void start(PolarisServerStartupContext context) {
    opa = new OpaContainer();
    opa.start();
    opa.uploadRegoPolicy(POLICY_NAME, POLICY);
    String base = opa.getExternalUrl() + "v1/data/" + POLICY_NAME;
    context.getSystemProperties().put("polaris.authorization.opa.policy-uri", base);
    context
        .getSystemProperties()
        .put("polaris.authorization.opa.batch-policy-uri", base + "/batch_allow");
    context.getSystemProperties().put("polaris.authorization.opa.max-batch-size", MAX_BATCH_SIZE);
  }

  @Override
  public void close() {
    if (opa != null) {
      opa.stop();
      opa = null;
    }
  }
}

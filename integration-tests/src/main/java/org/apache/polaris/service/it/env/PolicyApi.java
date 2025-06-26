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
package org.apache.polaris.service.it.env;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.policy.exceptions.PolicyInUseException;
import org.apache.polaris.service.types.ApplicablePolicy;
import org.apache.polaris.service.types.AttachPolicyRequest;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.DetachPolicyRequest;
import org.apache.polaris.service.types.GetApplicablePoliciesResponse;
import org.apache.polaris.service.types.ListPoliciesResponse;
import org.apache.polaris.service.types.LoadPolicyResponse;
import org.apache.polaris.service.types.Policy;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.apache.polaris.service.types.UpdatePolicyRequest;
import org.assertj.core.api.Assertions;

public class PolicyApi extends RestApi {
  PolicyApi(Client client, PolarisApiEndpoints endpoints, String authToken, URI uri) {
    super(client, endpoints, authToken, uri);
  }

  public void purge(String catalog, Namespace ns) {
    listPolicies(catalog, ns).forEach(t -> dropPolicy(catalog, t));
  }

  public List<PolicyIdentifier> listPolicies(String catalog, Namespace namespace) {
    return listPolicies(catalog, namespace, null);
  }

  public List<PolicyIdentifier> listPolicies(String catalog, Namespace namespace, PolicyType type) {
    String ns = RESTUtil.encodeNamespace(namespace);
    Map<String, String> queryParams = new HashMap<>();
    if (type != null) {
      queryParams.put("policyType", type.getName());
    }
    try (Response res =
        request(
                "polaris/v1/{cat}/namespaces/{ns}/policies",
                Map.of("cat", catalog, "ns", ns),
                queryParams)
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(ListPoliciesResponse.class).getIdentifiers().stream().toList();
    }
  }

  public void dropPolicy(String catalog, PolicyIdentifier policyIdentifier) {
    dropPolicy(catalog, policyIdentifier, null);
  }

  public void dropPolicy(String catalog, PolicyIdentifier policyIdentifier, Boolean detachAll) {
    String ns = RESTUtil.encodeNamespace(policyIdentifier.getNamespace());
    Map<String, String> queryParams = new HashMap<>();
    if (detachAll != null) {
      queryParams.put("detach-all", detachAll.toString());
    }
    try (Response res =
        request(
                "polaris/v1/{cat}/namespaces/{ns}/policies/{policy}",
                Map.of("cat", catalog, "ns", ns, "policy", policyIdentifier.getName()),
                queryParams)
            .delete()) {
      if (res.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()) {
        throw new PolicyInUseException("Policy in use");
      }
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
  }

  public Policy loadPolicy(String catalog, PolicyIdentifier policyIdentifier) {
    String ns = RESTUtil.encodeNamespace(policyIdentifier.getNamespace());
    try (Response res =
        request(
                "polaris/v1/{cat}/namespaces/{ns}/policies/{policy}",
                Map.of("cat", catalog, "ns", ns, "policy", policyIdentifier.getName()))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(LoadPolicyResponse.class).getPolicy();
    }
  }

  public Policy createPolicy(
      String catalog,
      PolicyIdentifier policyIdentifier,
      PolicyType policyType,
      String content,
      String description) {
    String ns = RESTUtil.encodeNamespace(policyIdentifier.getNamespace());
    CreatePolicyRequest request =
        CreatePolicyRequest.builder()
            .setType(policyType.getName())
            .setName(policyIdentifier.getName())
            .setDescription(description)
            .setContent(content)
            .build();
    try (Response res =
        request("polaris/v1/{cat}/namespaces/{ns}/policies", Map.of("cat", catalog, "ns", ns))
            .post(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(LoadPolicyResponse.class).getPolicy();
    }
  }

  public Policy updatePolicy(
      String catalog,
      PolicyIdentifier policyIdentifier,
      String newContent,
      String newDescription,
      int currentPolicyVersion) {
    String ns = RESTUtil.encodeNamespace(policyIdentifier.getNamespace());
    UpdatePolicyRequest request =
        UpdatePolicyRequest.builder()
            .setContent(newContent)
            .setDescription(newDescription)
            .setCurrentPolicyVersion(currentPolicyVersion)
            .build();
    try (Response res =
        request(
                "polaris/v1/{cat}/namespaces/{ns}/policies/{policy}",
                Map.of("cat", catalog, "ns", ns, "policy", policyIdentifier.getName()))
            .put(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(LoadPolicyResponse.class).getPolicy();
    }
  }

  public void attachPolicy(
      String catalog,
      PolicyIdentifier policyIdentifier,
      PolicyAttachmentTarget target,
      Map<String, String> parameters) {
    String ns = RESTUtil.encodeNamespace(policyIdentifier.getNamespace());
    AttachPolicyRequest request =
        AttachPolicyRequest.builder().setTarget(target).setParameters(parameters).build();
    try (Response res =
        request(
                "polaris/v1/{cat}/namespaces/{ns}/policies/{policy}/mappings",
                Map.of("cat", catalog, "ns", ns, "policy", policyIdentifier.getName()))
            .put(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
  }

  public void detachPolicy(
      String catalog, PolicyIdentifier policyIdentifier, PolicyAttachmentTarget target) {
    String ns = RESTUtil.encodeNamespace(policyIdentifier.getNamespace());
    DetachPolicyRequest request = DetachPolicyRequest.builder().setTarget(target).build();
    try (Response res =
        request(
                "polaris/v1/{cat}/namespaces/{ns}/policies/{policy}/mappings",
                Map.of("cat", catalog, "ns", ns, "policy", policyIdentifier.getName()))
            .post(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
  }

  public List<ApplicablePolicy> getApplicablePolicies(
      String catalog, Namespace namespace, String targetName, PolicyType policyType) {
    String ns = namespace != null ? RESTUtil.encodeNamespace(namespace) : null;
    Map<String, String> queryParams = new HashMap<>();
    if (ns != null) {
      queryParams.put("namespace", ns);
    }
    if (targetName != null) {
      queryParams.put("target-name", targetName);
    }
    if (policyType != null) {
      queryParams.put("policyType", policyType.getName());
    }

    try (Response res =
        request("polaris/v1/{cat}/applicable-policies", Map.of("cat", catalog), queryParams)
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(GetApplicablePoliciesResponse.class).getApplicablePolicies().stream()
          .toList();
    }
  }
}

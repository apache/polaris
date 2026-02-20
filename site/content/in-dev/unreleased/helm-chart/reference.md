---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: Values Reference
linkTitle: Values Reference
type: docs
weight: 900
---

<h3>Deployment</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>replicaCount</td>
			<td>int</td>
			<td><pre lang="json">
1
</pre>
</td>
			<td>The number of replicas to deploy (horizontal scaling). Beware that replicas are stateless; don't set this number > 1 when using in-memory meta store manager. See <a href="{{% ref "production/#scaling" %}}">Scaling</a> for production recommendations.</td>
		</tr>
		<tr>
			<td>revisionHistoryLimit</td>
			<td>int</td>
			<td><pre lang="json">
null
</pre>
</td>
			<td>The number of old ReplicaSets to retain to allow rollback (if not set, the default Kubernetes value is set to 10).</td>
		</tr>
	</tbody>
</table>
<h3>Image</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>image.repository</td>
			<td>string</td>
			<td><pre lang="json">
"apache/polaris"
</pre>
</td>
			<td>The image repository to pull from.</td>
		</tr>
		<tr>
			<td>image.pullPolicy</td>
			<td>string</td>
			<td><pre lang="json">
"IfNotPresent"
</pre>
</td>
			<td>The image pull policy.</td>
		</tr>
		<tr>
			<td>image.tag</td>
			<td>string</td>
			<td><pre lang="json">
"latest"
</pre>
</td>
			<td>The image tag.</td>
		</tr>
		<tr>
			<td>image.configDir</td>
			<td>string</td>
			<td><pre lang="json">
"/deployments/config"
</pre>
</td>
			<td>The path to the directory where the application.properties file, and other configuration files, if any, should be mounted.</td>
		</tr>
		<tr>
			<td>imagePullSecrets</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>References to secrets in the same namespace to use for pulling any of the images used by this chart. Each entry is a string referring to an existing secret in the namespace. The secret must contain a .dockerconfigjson key with a base64-encoded Docker configuration file. See <a href="https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/">Pulling from Private Registry</a> for more information.</td>
		</tr>
	</tbody>
</table>
<h3>Service Account</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>serviceAccount.create</td>
			<td>bool</td>
			<td><pre lang="json">
true
</pre>
</td>
			<td>Specifies whether a service account should be created.</td>
		</tr>
		<tr>
			<td>serviceAccount.annotations</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Annotations to add to the service account.</td>
		</tr>
		<tr>
			<td>serviceAccount.name</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The name of the service account to use. If not set and create is true, a name is generated using the fullname template.</td>
		</tr>
	</tbody>
</table>
<h3>Pod Configuration</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>podAnnotations</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Annotations to apply to polaris pods.</td>
		</tr>
		<tr>
			<td>podLabels</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Additional Labels to apply to polaris pods.</td>
		</tr>
		<tr>
			<td>configMapLabels</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Additional Labels to apply to polaris configmap.</td>
		</tr>
		<tr>
			<td>podDisruptionBudget.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Specifies whether a pod disruption budget should be created.</td>
		</tr>
		<tr>
			<td>podDisruptionBudget.minAvailable</td>
			<td>int</td>
			<td><pre lang="json">
0
</pre>
</td>
			<td>The minimum number of pods that should remain available during disruptions. Can be an absolute number (ex: 5) or a percentage of desired pods (ex: 50%). IMPORTANT: Cannot be used simultaneously with maxUnavailable.</td>
		</tr>
		<tr>
			<td>podDisruptionBudget.maxUnavailable</td>
			<td>int</td>
			<td><pre lang="json">
0
</pre>
</td>
			<td>The maximum number of pods that can be unavailable during disruptions. Can be an absolute number (ex: 5) or a percentage of desired pods (ex: 50%). IMPORTANT: Cannot be used simultaneously with minAvailable.</td>
		</tr>
		<tr>
			<td>podDisruptionBudget.annotations</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Annotations to add to the pod disruption budget.</td>
		</tr>
		<tr>
			<td>podSecurityContext</td>
			<td>object</td>
			<td><pre lang="json">
{
  "fsGroup": 10001,
  "seccompProfile": {
    "type": "RuntimeDefault"
  }
}
</pre>
</td>
			<td>Security context for the polaris pod. See <a href="https://kubernetes.io/docs/tasks/configure-pod-container/security-context/">Security Context</a>.</td>
		</tr>
		<tr>
			<td>containerSecurityContext</td>
			<td>object</td>
			<td><pre lang="json">
{
  "allowPrivilegeEscalation": false,
  "capabilities": {
    "drop": [
      "ALL"
    ]
  },
  "runAsNonRoot": true,
  "runAsUser": 10000,
  "seccompProfile": {
    "type": "RuntimeDefault"
  }
}
</pre>
</td>
			<td>Security context for the polaris container. See <a href="https://kubernetes.io/docs/tasks/configure-pod-container/security-context/">Security Context</a>.</td>
		</tr>
	</tbody>
</table>
<h3>Service</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>service.type</td>
			<td>string</td>
			<td><pre lang="json">
"ClusterIP"
</pre>
</td>
			<td>The type of service to create. Valid values are: ExternalName, ClusterIP, NodePort, and LoadBalancer. The default value is ClusterIP. See <a href="{{% ref "networking" %}}">Networking</a> for more information.</td>
		</tr>
		<tr>
			<td>service.ports[0].name</td>
			<td>string</td>
			<td><pre lang="json">
"polaris-http"
</pre>
</td>
			<td>The name of the port. Required.</td>
		</tr>
		<tr>
			<td>service.ports[0].port</td>
			<td>int</td>
			<td><pre lang="json">
8181
</pre>
</td>
			<td>The port the service listens on. By default, the HTTP port is 8181.</td>
		</tr>
		<tr>
			<td>service.ports[0].targetPort</td>
			<td>int</td>
			<td><pre lang="json">
0
</pre>
</td>
			<td>Number of the port to access on the pods targeted by the service. If this is not specified or zero, the value of the 'port' field is used.</td>
		</tr>
		<tr>
			<td>service.ports[0].nodePort</td>
			<td>int</td>
			<td><pre lang="json">
0
</pre>
</td>
			<td>The port on each node on which this service is exposed when type is NodePort or LoadBalancer. Usually assigned by the system. If not specified or zero, a port will be allocated if this Service requires one. If this field is specified when creating a Service which does not need it, creation will fail.</td>
		</tr>
		<tr>
			<td>service.ports[0].protocol</td>
			<td>string</td>
			<td><pre lang="json">
"TCP"
</pre>
</td>
			<td>The IP protocol for this port. Supports "TCP", "UDP", and "SCTP". Default is TCP.</td>
		</tr>
		<tr>
			<td>service.sessionAffinity</td>
			<td>string</td>
			<td><pre lang="json">
"None"
</pre>
</td>
			<td>The session affinity for the service. Valid values are: None, ClientIP. The default value is None. ClientIP enables sticky sessions based on the client's IP address. This is generally beneficial to Polaris deployments, but some testing may be required in order to make sure that the load is distributed evenly among the pods. Also, this setting affects only internal clients, not external ones. If Ingress is enabled, it is recommended to set sessionAffinity to None.</td>
		</tr>
		<tr>
			<td>service.clusterIP</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>You can specify your own cluster IP address If you define a Service that has the .spec.clusterIP set to "None" then Kubernetes does not assign an IP address. Instead, DNS records for the service will return the IP addresses of each pod targeted by the server. This is called a headless service. See <a href="https://kubernetes.io/docs/concepts/services-networking/service/#headless-services">Headless Services</a>.</td>
		</tr>
		<tr>
			<td>service.internalTrafficPolicy</td>
			<td>string</td>
			<td><pre lang="json">
"Cluster"
</pre>
</td>
			<td>Controls how traffic from internal sources is routed. Valid values are Cluster and Local. The default value is Cluster. Set the field to Cluster to route traffic to all ready endpoints. Set the field to Local to only route to ready node-local endpoints. If the traffic policy is Local and there are no node-local endpoints, traffic is dropped by kube-proxy.</td>
		</tr>
		<tr>
			<td>service.externalTrafficPolicy</td>
			<td>string</td>
			<td><pre lang="json">
"Cluster"
</pre>
</td>
			<td>Controls how traffic from external sources is routed. Valid values are Cluster and Local. The default value is Cluster. Set the field to Cluster to route traffic to all ready endpoints. Set the field to Local to only route to ready node-local endpoints. If the traffic policy is Local and there are no node-local endpoints, traffic is dropped by kube-proxy.</td>
		</tr>
		<tr>
			<td>service.trafficDistribution</td>
			<td>string</td>
			<td><pre lang="json">
null
</pre>
</td>
			<td>The traffic distribution field provides another way to influence traffic routing within a Kubernetes Service. While traffic policies focus on strict semantic guarantees, traffic distribution allows you to express preferences such as routing to topologically closer endpoints. The only valid value is: PreferClose. The default value is implementation-specific.</td>
		</tr>
		<tr>
			<td>service.annotations</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Annotations to add to the service.</td>
		</tr>
	</tbody>
</table>
<h3>Management Service</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>managementService.type</td>
			<td>string</td>
			<td><pre lang="json">
"ClusterIP"
</pre>
</td>
			<td>The type of service to create. Valid values are: ExternalName, ClusterIP, NodePort, and LoadBalancer. The default value is ClusterIP.</td>
		</tr>
		<tr>
			<td>managementService.ports[0].name</td>
			<td>string</td>
			<td><pre lang="json">
"polaris-mgmt"
</pre>
</td>
			<td>The name of the management port. Required.</td>
		</tr>
		<tr>
			<td>managementService.ports[0].port</td>
			<td>int</td>
			<td><pre lang="json">
8182
</pre>
</td>
			<td>The port the management service listens on. By default, the management interface is exposed on HTTP port 8182.</td>
		</tr>
		<tr>
			<td>managementService.ports[0].targetPort</td>
			<td>int</td>
			<td><pre lang="json">
0
</pre>
</td>
			<td>Number of the port to access on the pods targeted by the service. If this is not specified or zero, the value of the 'port' field is used.</td>
		</tr>
		<tr>
			<td>managementService.ports[0].nodePort</td>
			<td>int</td>
			<td><pre lang="json">
0
</pre>
</td>
			<td>The port on each node on which this service is exposed when type is NodePort or LoadBalancer. Usually assigned by the system. If not specified or zero, a port will be allocated if this Service requires one. If this field is specified when creating a Service which does not need it, creation will fail.</td>
		</tr>
		<tr>
			<td>managementService.ports[0].protocol</td>
			<td>string</td>
			<td><pre lang="json">
"TCP"
</pre>
</td>
			<td>The IP protocol for this port. Supports "TCP", "UDP", and "SCTP". Default is TCP.</td>
		</tr>
		<tr>
			<td>managementService.clusterIP</td>
			<td>string</td>
			<td><pre lang="json">
"None"
</pre>
</td>
			<td>By default, the management service is headless, i.e. it does not have a cluster IP. This is generally the right option for exposing health checks and metrics, e.g. for metrics scraping and service monitoring.</td>
		</tr>
		<tr>
			<td>managementService.sessionAffinity</td>
			<td>string</td>
			<td><pre lang="json">
"None"
</pre>
</td>
			<td>The session affinity for the service.</td>
		</tr>
		<tr>
			<td>managementService.internalTrafficPolicy</td>
			<td>string</td>
			<td><pre lang="json">
"Cluster"
</pre>
</td>
			<td>Controls how traffic from internal sources is routed.</td>
		</tr>
		<tr>
			<td>managementService.externalTrafficPolicy</td>
			<td>string</td>
			<td><pre lang="json">
"Cluster"
</pre>
</td>
			<td>Controls how traffic from external sources is routed.</td>
		</tr>
		<tr>
			<td>managementService.trafficDistribution</td>
			<td>string</td>
			<td><pre lang="json">
null
</pre>
</td>
			<td>The traffic distribution field.</td>
		</tr>
		<tr>
			<td>managementService.annotations</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Annotations to add to the service.</td>
		</tr>
	</tbody>
</table>
<h3>Extra Services</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>extraServices[0].nameSuffix</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The suffix to append to the service name. Required. It must be unique. If it does not start with a hyphen, a hyphen will be inserted between the base service name and the suffix.</td>
		</tr>
		<tr>
			<td>extraServices[0].type</td>
			<td>string</td>
			<td><pre lang="json">
"LoadBalancer"
</pre>
</td>
			<td>The type of service to create. Valid values are: ExternalName, ClusterIP, NodePort, and LoadBalancer.</td>
		</tr>
		<tr>
			<td>extraServices[0].ports[0].name</td>
			<td>string</td>
			<td><pre lang="json">
"polaris-extra"
</pre>
</td>
			<td>The name of the port. Required.</td>
		</tr>
		<tr>
			<td>extraServices[0].ports[0].port</td>
			<td>int</td>
			<td><pre lang="json">
8183
</pre>
</td>
			<td>The port the extra service listens on.</td>
		</tr>
		<tr>
			<td>extraServices[0].ports[0].targetPort</td>
			<td>int</td>
			<td><pre lang="json">
0
</pre>
</td>
			<td>Number of the port to access on the pods targeted by the service. If this is not specified or zero, the value of the 'port' field is used.</td>
		</tr>
		<tr>
			<td>extraServices[0].ports[0].nodePort</td>
			<td>int</td>
			<td><pre lang="json">
0
</pre>
</td>
			<td>The port on each node on which this extra service is exposed when type is NodePort or LoadBalancer. Usually assigned by the system. If not specified or zero, a port will be allocated if this Service requires one. If this field is specified when creating a Service which does not need it, creation will fail.</td>
		</tr>
		<tr>
			<td>extraServices[0].ports[0].protocol</td>
			<td>string</td>
			<td><pre lang="json">
"TCP"
</pre>
</td>
			<td>The IP protocol for this port. Supports "TCP", "UDP", and "SCTP". Default is TCP.</td>
		</tr>
		<tr>
			<td>extraServices[0].clusterIP</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The cluster IP for the extra service.</td>
		</tr>
		<tr>
			<td>extraServices[0].sessionAffinity</td>
			<td>string</td>
			<td><pre lang="json">
"None"
</pre>
</td>
			<td>The session affinity for the extra service. Valid values are: None, ClientIP. The default value is None.</td>
		</tr>
		<tr>
			<td>extraServices[0].internalTrafficPolicy</td>
			<td>string</td>
			<td><pre lang="json">
"Cluster"
</pre>
</td>
			<td>Controls how traffic from internal sources is routed.</td>
		</tr>
		<tr>
			<td>extraServices[0].externalTrafficPolicy</td>
			<td>string</td>
			<td><pre lang="json">
"Cluster"
</pre>
</td>
			<td>Controls how traffic from external sources is routed.</td>
		</tr>
		<tr>
			<td>extraServices[0].trafficDistribution</td>
			<td>string</td>
			<td><pre lang="json">
null
</pre>
</td>
			<td>The traffic distribution field.</td>
		</tr>
		<tr>
			<td>extraServices[0].annotations</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Annotations to add to the extra service.</td>
		</tr>
	</tbody>
</table>
<h3>Ingress</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>ingress.className</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>Specifies the ingressClassName; leave empty if you don't want to customize it. See <a href="{{% ref "networking" %}}">Networking</a> for more information.</td>
		</tr>
		<tr>
			<td>ingress.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Specifies whether an ingress should be created.</td>
		</tr>
		<tr>
			<td>ingress.annotations</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Annotations to add to the ingress.</td>
		</tr>
		<tr>
			<td>ingress.hosts[0].host</td>
			<td>string</td>
			<td><pre lang="json">
"chart-example.local"
</pre>
</td>
			<td>The host name. Required.</td>
		</tr>
		<tr>
			<td>ingress.hosts[0].paths[0].path</td>
			<td>string</td>
			<td><pre lang="json">
"/"
</pre>
</td>
			<td>The path to match.</td>
		</tr>
		<tr>
			<td>ingress.hosts[0].paths[0].pathType</td>
			<td>string</td>
			<td><pre lang="json">
"Prefix"
</pre>
</td>
			<td>The type of path. Valid values are: Exact, Prefix, and ImplementationSpecific.</td>
		</tr>
		<tr>
			<td>ingress.tls[0].secretName</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The name of the TLS secret to use to terminate TLS traffic on port 443. Required.</td>
		</tr>
		<tr>
			<td>ingress.tls[0].hosts</td>
			<td>list</td>
			<td><pre lang="json">
[
  "chart-example1.local",
  "chart-example2.local"
]
</pre>
</td>
			<td>A list of hosts in the certificate.</td>
		</tr>
	</tbody>
</table>
<h3>Gateway</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>gateway.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Specifies whether a Gateway should be created. See <a href="{{% ref "networking" %}}">Networking</a> for more information.</td>
		</tr>
		<tr>
			<td>gateway.annotations</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Annotations to add to the Gateway.</td>
		</tr>
		<tr>
			<td>gateway.className</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The name of the GatewayClass to use.</td>
		</tr>
		<tr>
			<td>gateway.listeners[0].name</td>
			<td>string</td>
			<td><pre lang="json">
"http"
</pre>
</td>
			<td>The name of the listener. Required.</td>
		</tr>
		<tr>
			<td>gateway.listeners[0].protocol</td>
			<td>string</td>
			<td><pre lang="json">
"HTTP"
</pre>
</td>
			<td>Protocol specifies the network protocol this listener expects to receive.</td>
		</tr>
		<tr>
			<td>gateway.listeners[0].port</td>
			<td>int</td>
			<td><pre lang="json">
80
</pre>
</td>
			<td>The port number to use for the listener.</td>
		</tr>
		<tr>
			<td>gateway.listeners[0].hostname</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>Hostname specifies the virtual hostname to match for protocol types that define this concept. When unspecified, all hostnames are matched.</td>
		</tr>
		<tr>
			<td>gateway.listeners[0].allowedRoutes</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>AllowedRoutes defines the types of routes that MAY be attached to a Listener and the trusted namespaces where those Route resources MAY be present.  <p>Example:</p>  <pre> allowedRoutes:<br/>   namespaces:<br/>     from: Same</td>
		</tr>
		<tr>
			<td>gateway.addresses</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>Optional addresses to request for the Gateway.  <p>Example:</p>  <pre> addresses:<br/>   - type: IPAddress<br/>     value: 192.168.1.1 </pre></td>
		</tr>
	</tbody>
</table>
<h3>HTTPRoute</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>httproute.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Specifies whether an HTTPRoute should be created. See <a href="{{% ref "networking" %}}">Networking</a> for more information.</td>
		</tr>
		<tr>
			<td>httproute.annotations</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Annotations to add to the HTTPRoute.</td>
		</tr>
		<tr>
			<td>httproute.gatewayName</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>Name of the Gateway resource to attach to. Required.</td>
		</tr>
		<tr>
			<td>httproute.gatewayNamespace</td>
			<td>string</td>
			<td><pre lang="json">
"default"
</pre>
</td>
			<td>Namespace where the Gateway is deployed. Required.</td>
		</tr>
		<tr>
			<td>httproute.sectionName</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>Section name within the gateway to use (optional).</td>
		</tr>
		<tr>
			<td>httproute.hosts</td>
			<td>list</td>
			<td><pre lang="json">
[
  "chart-example.local"
]
</pre>
</td>
			<td>A list of hostnames that the HTTPRoute should match.</td>
		</tr>
	</tbody>
</table>
<h3>Resources and Autoscaling</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>resources</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Configures the resources requests and limits for polaris pods. This chart does not specify default resources and leaves this as a conscious choice for the user. This also increases chances charts run on environments with little resources, such as Minikube. See <a href="{{% ref "production/#resource-management" %}}">Resource Management</a> for production recommendations.  <p>Example:</p>  <pre> resources:<br/>   limits:<br/>     cpu: 2<br/>     memory: 256Mi<br/>   requests:<br/>     cpu: 4<br/>     memory: 512Mi </pre></td>
		</tr>
		<tr>
			<td>autoscaling.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Specifies whether automatic horizontal scaling should be enabled. Do not enable this when using in-memory version store type. See <a href="{{% ref "production/#scaling" %}}">Scaling</a> for production recommendations.</td>
		</tr>
		<tr>
			<td>autoscaling.minReplicas</td>
			<td>int</td>
			<td><pre lang="json">
1
</pre>
</td>
			<td>The minimum number of replicas to maintain.</td>
		</tr>
		<tr>
			<td>autoscaling.maxReplicas</td>
			<td>int</td>
			<td><pre lang="json">
3
</pre>
</td>
			<td>The maximum number of replicas to maintain.</td>
		</tr>
		<tr>
			<td>autoscaling.targetCPUUtilizationPercentage</td>
			<td>int</td>
			<td><pre lang="json">
80
</pre>
</td>
			<td>Optional; set to zero or empty to disable.</td>
		</tr>
		<tr>
			<td>autoscaling.targetMemoryUtilizationPercentage</td>
			<td>int</td>
			<td><pre lang="json">
0
</pre>
</td>
			<td>Optional; set to zero or empty to disable.</td>
		</tr>
	</tbody>
</table>
<h3>Scheduling</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>priorityClassName</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>Priority class name for polaris pods. See <a href="https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/#pod-priority">Pod Priority</a></td>
		</tr>
		<tr>
			<td>nodeSelector</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Node labels which must match for the polaris pod to be scheduled on that node. See <a href="https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector">Node Selector</a>.</td>
		</tr>
		<tr>
			<td>tolerations</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>A list of tolerations to apply to polaris pods. See <a href="https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/">Taints and Tolerations</a>.  <p>Example:</p>  <pre> tolerations:<br/>   - key: "node-role.kubernetes.io/control-plane"<br/>     operator: "Exists"<br/>     effect: "NoSchedule" </pre></td>
		</tr>
		<tr>
			<td>affinity</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Affinity and anti-affinity for polaris pods. See <a href="https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity">Affinity and Anti-Affinity</a>.  <p>Example:</p>  <pre> affinity:<br/>   podAffinity:<br/>     preferredDuringSchedulingIgnoredDuringExecution:<br/>       - weight: 100<br/>         podAffinityTerm:<br/>           topologyKey: kubernetes.io/hostname<br/>           labelSelector:<br/>             matchExpressions:<br/>               - key: app.kubernetes.io/name<br/>                 operator: In<br/>                 values:<br/>                   - polaris </pre></td>
		</tr>
		<tr>
			<td>topologySpreadConstraints</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>Topology spread constraints for polaris pods. See <a href="https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/#topologyspreadconstraints-field">Topology Spread Constraints</a>.  <p>Example:</p>  <pre> topologySpreadConstraints:<br/>   - maxSkew: 1<br/>     topologyKey: topology.kubernetes.io/zone<br/>     whenUnsatisfiable: DoNotSchedule </pre></td>
		</tr>
	</tbody>
</table>
<h3>Probes</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>livenessProbe.initialDelaySeconds</td>
			<td>int</td>
			<td><pre lang="json">
5
</pre>
</td>
			<td>Number of seconds after the container has started before liveness probes are initiated. Minimum value is 0.</td>
		</tr>
		<tr>
			<td>livenessProbe.periodSeconds</td>
			<td>int</td>
			<td><pre lang="json">
10
</pre>
</td>
			<td>How often (in seconds) to perform the probe. Minimum value is 1.</td>
		</tr>
		<tr>
			<td>livenessProbe.successThreshold</td>
			<td>int</td>
			<td><pre lang="json">
1
</pre>
</td>
			<td>Minimum consecutive successes for the probe to be considered successful after having failed. Minimum value is 1.</td>
		</tr>
		<tr>
			<td>livenessProbe.failureThreshold</td>
			<td>int</td>
			<td><pre lang="json">
3
</pre>
</td>
			<td>Minimum consecutive failures for the probe to be considered failed after having succeeded. Minimum value is 1.</td>
		</tr>
		<tr>
			<td>livenessProbe.timeoutSeconds</td>
			<td>int</td>
			<td><pre lang="json">
10
</pre>
</td>
			<td>Number of seconds after which the probe times out. Minimum value is 1.</td>
		</tr>
		<tr>
			<td>livenessProbe.terminationGracePeriodSeconds</td>
			<td>int</td>
			<td><pre lang="json">
30
</pre>
</td>
			<td>Optional duration in seconds the pod needs to terminate gracefully upon probe failure. Minimum value is 1.</td>
		</tr>
		<tr>
			<td>readinessProbe.initialDelaySeconds</td>
			<td>int</td>
			<td><pre lang="json">
5
</pre>
</td>
			<td>Number of seconds after the container has started before readiness probes are initiated. Minimum value is 0.</td>
		</tr>
		<tr>
			<td>readinessProbe.periodSeconds</td>
			<td>int</td>
			<td><pre lang="json">
10
</pre>
</td>
			<td>How often (in seconds) to perform the probe. Minimum value is 1.</td>
		</tr>
		<tr>
			<td>readinessProbe.successThreshold</td>
			<td>int</td>
			<td><pre lang="json">
1
</pre>
</td>
			<td>Minimum consecutive successes for the probe to be considered successful after having failed. Minimum value is 1.</td>
		</tr>
		<tr>
			<td>readinessProbe.failureThreshold</td>
			<td>int</td>
			<td><pre lang="json">
3
</pre>
</td>
			<td>Minimum consecutive failures for the probe to be considered failed after having succeeded. Minimum value is 1.</td>
		</tr>
		<tr>
			<td>readinessProbe.timeoutSeconds</td>
			<td>int</td>
			<td><pre lang="json">
10
</pre>
</td>
			<td>Number of seconds after which the probe times out. Minimum value is 1.</td>
		</tr>
	</tbody>
</table>
<h3>Advanced Configuration</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>advancedConfig</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Advanced configuration. You can pass here any valid Polaris or Quarkus configuration property. Any property that is defined here takes precedence over all the other configuration values generated by this chart. Properties can be passed "flattened" or as nested YAML objects (see examples below). <p>Examples:</p>  <p>Enable access log:</p>  <pre> advancedConfig:<br/>   quarkus.http.access-log.enabled: "true" </pre>  <p>Reverse proxy configuration:</p>  <pre> advancedConfig:<br/>   quarkus:<br/>     http:<br/>       proxy:<br/>         proxy-address-forwarding: "true"<br/>         allow-x-forwarded: "true"<br/>         enable-forwarded-host: "true"<br/>         enable-forwarded-prefix: "true"<br/>         trusted-proxies: "127.0.0.1" </pre>  <p>Note: the above config options are for documentation purposes only! Consult the <a href="https://quarkus.io/guides/http-reference#reverse-proxy">Quarkus Reverse Proxy</a> documentation and configure those depending on your actual needs.</p></td>
		</tr>
		<tr>
			<td>extraEnv</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>Advanced configuration via Environment Variables. Extra environment variables to add to the Polaris server container. You can pass here any valid EnvVar object: <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.27/#envvar-v1-core">EnvVar API</a> This can be useful to get configuration values from Kubernetes secrets or config maps.  <p>Example:</p>  <pre> extraEnv:<br/>   - name: AWS_STORAGE_BUCKET<br/>     value: s3://xxxxx/<br/>   - name: AWS_ACCESS_KEY_ID<br/>     valueFrom:<br/>       secretKeyRef:<br/>         name: aws-secret<br/>         key: access_key_id<br/>   - name: AWS_SECRET_ACCESS_KEY<br/>     valueFrom:<br/>       secretKeyRef:<br/>         name: aws-secret<br/>         key: secret_access_key </pre></td>
		</tr>
		<tr>
			<td>extraVolumes</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>Extra volumes to add to the polaris pod. See <a href="https://kubernetes.io/docs/concepts/storage/volumes/">Volumes</a>.  <p>Example:</p>  <pre> extraVolumes:<br/>   - name: extra-volume<br/>     emptyDir: {} </pre></td>
		</tr>
		<tr>
			<td>extraVolumeMounts</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>Extra volume mounts to add to the polaris container. See <a href="https://kubernetes.io/docs/concepts/storage/volumes/">Volumes</a>.  <p>Example:</p>  <pre> extraVolumeMounts:<br/>   - name: extra-volume<br/>     mountPath: /usr/share/extra-volume </pre></td>
		</tr>
		<tr>
			<td>extraInitContainers</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>Add additional init containers to the polaris pod(s) See <a href="https://kubernetes.io/docs/concepts/workloads/pods/init-containers/">Init Containers</a>.  <p>Example:</p>  <pre> extraInitContainers:<br/>   - name: your-image-name<br/>     image: your-image<br/>     imagePullPolicy: Always<br/>     command: ['sh', '-c', 'echo "hello world"'] </pre></td>
		</tr>
	</tbody>
</table>
<h3>Observability</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>tracing.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Specifies whether tracing for the polaris server should be enabled. See <a href="{{% ref "../telemetry/#traces" %}}">Traces</a> for details.</td>
		</tr>
		<tr>
			<td>tracing.endpoint</td>
			<td>string</td>
			<td><pre lang="json">
"http://otlp-collector:4317"
</pre>
</td>
			<td>The collector endpoint URL to connect to (required). The endpoint URL must have either the http:// or the https:// scheme. The collector must talk the OpenTelemetry protocol (OTLP) and the port must be its gRPC port (by default 4317). See <a href="https://quarkus.io/guides/opentelemetry">Quarkus OpenTelemetry</a> for more information.</td>
		</tr>
		<tr>
			<td>tracing.sample</td>
			<td>string</td>
			<td><pre lang="json">
"1.0d"
</pre>
</td>
			<td>Which requests should be sampled. Valid values are: "all", "none", or a ratio between 0.0 and "1.0d" (inclusive). E.g. "0.5d" means that 50% of the requests will be sampled. Note: avoid entering numbers here, always prefer a string representation of the ratio.</td>
		</tr>
		<tr>
			<td>tracing.attributes</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Resource attributes to identify the polaris service among other tracing sources. See <a href="https://opentelemetry.io/docs/reference/specification/resource/semantic_conventions/#service">OpenTelemetry Semantic Conventions</a>. If left empty, traces will be attached to a service named "Apache Polaris"; to change this, provide a service.name attribute here.</td>
		</tr>
		<tr>
			<td>metrics.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
true
</pre>
</td>
			<td>Specifies whether metrics for the polaris server should be enabled. See <a href="{{% ref "../telemetry/#metrics" %}}">Metrics</a> for details.</td>
		</tr>
		<tr>
			<td>metrics.tags</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Additional tags (dimensional labels) to add to the metrics.</td>
		</tr>
		<tr>
			<td>serviceMonitor.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
true
</pre>
</td>
			<td>Specifies whether a ServiceMonitor for Prometheus operator should be created.</td>
		</tr>
		<tr>
			<td>serviceMonitor.interval</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The scrape interval; leave empty to let Prometheus decide. Must be a valid duration, e.g. 1d, 1h30m, 5m, 10s.</td>
		</tr>
		<tr>
			<td>serviceMonitor.labels</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Labels for the created ServiceMonitor so that Prometheus operator can properly pick it up.  <p>Example:</p>  <pre> serviceMonitor:<br/>   labels:<br/>     release: prometheus </pre></td>
		</tr>
		<tr>
			<td>serviceMonitor.metricRelabelings</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>Relabeling rules to apply to metrics. Refer to <a href="https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config">relabel_config</a> for details.  <p>Example:</p>  <pre> serviceMonitor:<br/>   metricRelabelings:<br/>     - source_labels: [ __meta_kubernetes_namespace ]<br/>       separator: ;<br/>       regex: (.*)<br/>       target_label: namespace<br/>       replacement: $1<br/>       action: replace </pre></td>
		</tr>
	</tbody>
</table>
<h3>Logging</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>logging.level</td>
			<td>string</td>
			<td><pre lang="json">
"INFO"
</pre>
</td>
			<td>The log level of the root category, which is used as the default log level for all categories.</td>
		</tr>
		<tr>
			<td>logging.requestIdHeaderName</td>
			<td>string</td>
			<td><pre lang="json">
"X-Request-ID"
</pre>
</td>
			<td>The header name to use for the request ID.</td>
		</tr>
		<tr>
			<td>logging.console.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
true
</pre>
</td>
			<td>Whether to enable the console appender.</td>
		</tr>
		<tr>
			<td>logging.console.threshold</td>
			<td>string</td>
			<td><pre lang="json">
"ALL"
</pre>
</td>
			<td>The log level of the console appender.</td>
		</tr>
		<tr>
			<td>logging.console.json</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Whether to log in JSON format.</td>
		</tr>
		<tr>
			<td>logging.console.format</td>
			<td>string</td>
			<td><pre lang="json">
"%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c{3.}] [%X{requestId},%X{realmId}] [%X{traceId},%X{parentId},%X{spanId},%X{sampled}] (%t) %s%e%n"
</pre>
</td>
			<td>The log format to use. Ignored if JSON format is enabled. See <a href="{{% ref "../telemetry/#logging" %}}">Logging</a> for details.</td>
		</tr>
		<tr>
			<td>logging.file.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Whether to enable the file appender.</td>
		</tr>
		<tr>
			<td>logging.file.threshold</td>
			<td>string</td>
			<td><pre lang="json">
"ALL"
</pre>
</td>
			<td>The log level of the file appender.</td>
		</tr>
		<tr>
			<td>logging.file.json</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Whether to log in JSON format.</td>
		</tr>
		<tr>
			<td>logging.file.format</td>
			<td>string</td>
			<td><pre lang="json">
"%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c{3.}] [%X{requestId},%X{realmId}] [%X{traceId},%X{parentId},%X{spanId},%X{sampled}] (%t) %s%e%n"
</pre>
</td>
			<td>The log format to use. Ignored if JSON format is enabled. See <a href="{{% ref "../telemetry/#logging" %}}">Logging</a> for details.</td>
		</tr>
		<tr>
			<td>logging.file.logsDir</td>
			<td>string</td>
			<td><pre lang="json">
"/deployments/logs"
</pre>
</td>
			<td>The local directory where log files are stored. The persistent volume claim will be mounted here.</td>
		</tr>
		<tr>
			<td>logging.file.fileName</td>
			<td>string</td>
			<td><pre lang="json">
"polaris.log"
</pre>
</td>
			<td>The log file name.</td>
		</tr>
		<tr>
			<td>logging.file.rotation.maxFileSize</td>
			<td>string</td>
			<td><pre lang="json">
"100Mi"
</pre>
</td>
			<td>The maximum size of the log file before it is rotated. Should be expressed as a Kubernetes quantity.</td>
		</tr>
		<tr>
			<td>logging.file.rotation.maxBackupIndex</td>
			<td>int</td>
			<td><pre lang="json">
5
</pre>
</td>
			<td>The maximum number of backup files to keep.</td>
		</tr>
		<tr>
			<td>logging.file.rotation.fileSuffix</td>
			<td>string</td>
			<td><pre lang="json">
null
</pre>
</td>
			<td>An optional suffix to append to the rotated log files. If present, the rotated log files will be grouped in time buckets, and each bucket will contain at most maxBackupIndex files. The suffix must be in a date-time format that is understood by DateTimeFormatter. If the suffix ends with .gz or .zip, the rotated files will also be compressed using the corresponding algorithm.</td>
		</tr>
		<tr>
			<td>logging.file.storage.className</td>
			<td>string</td>
			<td><pre lang="json">
"standard"
</pre>
</td>
			<td>The storage class name of the persistent volume claim to create.</td>
		</tr>
		<tr>
			<td>logging.file.storage.size</td>
			<td>string</td>
			<td><pre lang="json">
"512Gi"
</pre>
</td>
			<td>The size of the persistent volume claim to create.</td>
		</tr>
		<tr>
			<td>logging.file.storage.selectorLabels</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Labels to add to the persistent volume claim spec selector; a persistent volume with matching labels must exist. Leave empty if using dynamic provisioning.</td>
		</tr>
		<tr>
			<td>logging.categories</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Configuration for specific log categories. Keys are category names (e.g., org.apache.polaris), values are log levels.</td>
		</tr>
		<tr>
			<td>logging.mdc</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Configuration for MDC (Mapped Diagnostic Context). Values specified here will be added to the log context of all incoming requests and can be used in log patterns.  <p>Example:</p>  <pre> mdc:<br/>   app: polaris<br/>   env: dev<br/>   service: polaris-service<br/> </pre></td>
		</tr>
	</tbody>
</table>
<h3>Realm Context</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>realmContext.type</td>
			<td>string</td>
			<td><pre lang="json">
"default"
</pre>
</td>
			<td>The type of realm context resolver to use. Two built-in types are supported: default and test; test is not recommended for production as it does not perform any realm validation.</td>
		</tr>
		<tr>
			<td>realmContext.realms</td>
			<td>list</td>
			<td><pre lang="json">
[
  "POLARIS"
]
</pre>
</td>
			<td>List of valid realms, for use with the default realm context resolver. The first realm in the list is the default realm. Realms not in this list will be rejected.</td>
		</tr>
	</tbody>
</table>
<h3>Features</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>features</td>
			<td>object</td>
			<td><pre lang="json">
{
  "realmOverrides": {}
}
</pre>
</td>
			<td>Features to enable or disable globally. If a feature is not present in the map, the default built-in value is used.  <p>Example:</p>  <pre> features:<br/>   ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING: false<br/>   SUPPORTED_CATALOG_STORAGE_TYPES:<br/>     - S3<br/>     - GCS<br/>     - AZURE </pre></td>
		</tr>
		<tr>
			<td>features.realmOverrides</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Features to enable or disable per realm. This field is a map of maps. The realm name is the key, and the value is a map of feature names to values. If a feature is not present in the map, the global value is used.  <p>Example:</p>  <pre> features:<br/>   ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING: false<br/>   SUPPORTED_CATALOG_STORAGE_TYPES:<br/>     - S3<br/>     - GCS<br/>     - AZURE<br/>   realmOverrides:<br/>     my-realm:<br/>       ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING: true<br/>     my-other-realm:<br/>       SUPPORTED_CATALOG_STORAGE_TYPES:<br/>         - S3 </pre></td>
		</tr>
	</tbody>
</table>
<h3>Persistence</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>persistence.type</td>
			<td>string</td>
			<td><pre lang="json">
"in-memory"
</pre>
</td>
			<td>The type of persistence to use. Three built-in types are supported: in-memory, relational-jdbc, and nosql (beta).</td>
		</tr>
		<tr>
			<td>persistence.relationalJdbc.secret.name</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The secret name to pull database connection properties from</td>
		</tr>
		<tr>
			<td>persistence.relationalJdbc.secret.username</td>
			<td>string</td>
			<td><pre lang="json">
"username"
</pre>
</td>
			<td>The secret key holding the database username for authentication</td>
		</tr>
		<tr>
			<td>persistence.relationalJdbc.secret.password</td>
			<td>string</td>
			<td><pre lang="json">
"password"
</pre>
</td>
			<td>The secret key holding the database password for authentication</td>
		</tr>
		<tr>
			<td>persistence.relationalJdbc.secret.jdbcUrl</td>
			<td>string</td>
			<td><pre lang="json">
"jdbcUrl"
</pre>
</td>
			<td>The secret key holding the database JDBC connection URL</td>
		</tr>
		<tr>
			<td>persistence.nosql.backend</td>
			<td>string</td>
			<td><pre lang="json">
"MongoDb"
</pre>
</td>
			<td>The NoSQL backend to use. Two built-in types are supported: MongoDb and InMemory. Only MongoDb is supported for production use.</td>
		</tr>
		<tr>
			<td>persistence.nosql.database</td>
			<td>string</td>
			<td><pre lang="json">
"polaris"
</pre>
</td>
			<td>The MongoDB database name to use.</td>
		</tr>
		<tr>
			<td>persistence.nosql.secret.name</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The secret name to pull the MongoDB connection string from.</td>
		</tr>
		<tr>
			<td>persistence.nosql.secret.connectionString</td>
			<td>string</td>
			<td><pre lang="json">
"connectionString"
</pre>
</td>
			<td>The secret key holding the MongoDB connection string.</td>
		</tr>
	</tbody>
</table>
<h3>File IO</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>fileIo.type</td>
			<td>string</td>
			<td><pre lang="json">
"default"
</pre>
</td>
			<td>The type of file IO to use. Two built-in types are supported: default and wasb. The wasb one translates WASB paths to ABFS ones.</td>
		</tr>
	</tbody>
</table>
<h3>Storage</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>storage.secret.name</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The name of the secret to pull storage credentials from.</td>
		</tr>
		<tr>
			<td>storage.secret.awsAccessKeyId</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The key in the secret to pull the AWS access key ID from. Only required when using AWS.</td>
		</tr>
		<tr>
			<td>storage.secret.awsSecretAccessKey</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The key in the secret to pull the AWS secret access key from. Only required when using AWS.</td>
		</tr>
		<tr>
			<td>storage.secret.gcpToken</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The key in the secret to pull the GCP token from. Only required when using GCP.</td>
		</tr>
		<tr>
			<td>storage.secret.gcpTokenLifespan</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The key in the secret to pull the GCP token expiration time from. Only required when using GCP. Must be a valid ISO 8601 duration. The default is PT1H (1 hour).</td>
		</tr>
	</tbody>
</table>
<h3>Authentication</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>authentication.type</td>
			<td>string</td>
			<td><pre lang="json">
"internal"
</pre>
</td>
			<td>The type of authentication to use. Three built-in types are supported: internal, external, and mixed.</td>
		</tr>
		<tr>
			<td>authentication.authenticator.type</td>
			<td>string</td>
			<td><pre lang="json">
"default"
</pre>
</td>
			<td>The type of authenticator to use.</td>
		</tr>
		<tr>
			<td>authentication.tokenService.type</td>
			<td>string</td>
			<td><pre lang="json">
"default"
</pre>
</td>
			<td>The token service implementation to use. Two built-in types are supported: default and disabled. Only relevant when using internal (or mixed) authentication. When using external authentication, the token service is always disabled.</td>
		</tr>
		<tr>
			<td>authentication.tokenBroker.type</td>
			<td>string</td>
			<td><pre lang="json">
"rsa-key-pair"
</pre>
</td>
			<td>The token broker implementation to use. Two built-in types are supported: rsa-key-pair and symmetric-key. Only relevant when using internal (or mixed) authentication. When using external authentication, the token broker is not used.</td>
		</tr>
		<tr>
			<td>authentication.tokenBroker.maxTokenGeneration</td>
			<td>string</td>
			<td><pre lang="json">
"PT1H"
</pre>
</td>
			<td>Maximum token generation duration (e.g., PT1H for 1 hour).</td>
		</tr>
		<tr>
			<td>authentication.tokenBroker.secret.name</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The name of the secret to pull the keys from. If not provided, a key pair will be generated. This is not recommended for production.</td>
		</tr>
		<tr>
			<td>authentication.tokenBroker.secret.rsaKeyPair.publicKey</td>
			<td>string</td>
			<td><pre lang="json">
"public.pem"
</pre>
</td>
			<td>Key name inside the secret for the RSA public key.</td>
		</tr>
		<tr>
			<td>authentication.tokenBroker.secret.rsaKeyPair.privateKey</td>
			<td>string</td>
			<td><pre lang="json">
"private.pem"
</pre>
</td>
			<td>Key name inside the secret for the RSA private key.</td>
		</tr>
		<tr>
			<td>authentication.tokenBroker.secret.symmetricKey.secretKey</td>
			<td>string</td>
			<td><pre lang="json">
"symmetric.key"
</pre>
</td>
			<td>Key name inside the secret for the symmetric key.</td>
		</tr>
		<tr>
			<td>authentication.realmOverrides</td>
			<td>object</td>
			<td><pre lang="json">
{}
</pre>
</td>
			<td>Authentication configuration overrides per realm.  <p>Example:</p>  <pre> authentication:<br/>   realmOverrides:<br/>     my-realm:<br/>       type: external<br/>       authenticator:<br/>         type: custom<br/>     my-other-realm:<br/>       type: mixed </pre></td>
		</tr>
	</tbody>
</table>
<h3>OIDC</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>oidc.authServeUrl</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The authentication server URL. Must be provided if at least one realm is configured for external authentication.</td>
		</tr>
		<tr>
			<td>oidc.client.id</td>
			<td>string</td>
			<td><pre lang="json">
"polaris"
</pre>
</td>
			<td>The client ID to use when contacting the authentication server's introspection endpoint in order to validate tokens.</td>
		</tr>
		<tr>
			<td>oidc.client.secret.name</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The name of the secret to pull the client secret from. If not provided, the client is assumed to not require a client secret when contacting the introspection endpoint.</td>
		</tr>
		<tr>
			<td>oidc.client.secret.key</td>
			<td>string</td>
			<td><pre lang="json">
"clientSecret"
</pre>
</td>
			<td>The key name inside the secret to pull the client secret from.</td>
		</tr>
		<tr>
			<td>oidc.principalMapper.type</td>
			<td>string</td>
			<td><pre lang="json">
"default"
</pre>
</td>
			<td>The <code>PrincipalMapper</code> implementation to use. Only one built-in type is supported: default.</td>
		</tr>
		<tr>
			<td>oidc.principalMapper.idClaimPath</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The path to the claim that contains the principal ID. Nested paths can be expressed using "/" as a separator, e.g. "polaris/principal_id" would look for the "principal_id" field inside the "polaris" object in the token claims. Optional. Either this option or <code>nameClaimPath</code> (or both) must be provided.</td>
		</tr>
		<tr>
			<td>oidc.principalMapper.nameClaimPath</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The claim that contains the principal name. Nested paths can be expressed using "/" as a separator, e.g. "polaris/principal_name" would look for the "principal_name" field inside the "polaris" object in the token claims. Optional. Either this option or <code>idClaimPath</code> (or both) must be provided.</td>
		</tr>
		<tr>
			<td>oidc.principalRolesMapper.type</td>
			<td>string</td>
			<td><pre lang="json">
"default"
</pre>
</td>
			<td>The <code>PrincipalRolesMapper</code> implementation to use. Only one built-in type is supported: default.</td>
		</tr>
		<tr>
			<td>oidc.principalRolesMapper.rolesClaimPath</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The path to the claim that contains the principal roles. Nested paths can be expressed using "/" as a separator, e.g. "polaris/principal_roles" would look for the "principal_roles" field inside the "polaris" object in the token claims. If not set, Quarkus looks for roles in standard locations. See <a href="https://quarkus.io/guides/security-oidc-bearer-token-authentication#token-claims-and-security-identity-roles">Quarkus OIDC Token Claims</a>.</td>
		</tr>
		<tr>
			<td>oidc.principalRolesMapper.filter</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>A regular expression that matches the role names in the identity. Only roles that match this regex will be included in the Polaris-specific roles.</td>
		</tr>
		<tr>
			<td>oidc.principalRolesMapper.mappings</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>A list of regex mappings that will be applied to each role name in the identity. This can be used to transform the role names in the identity into role names as expected by Polaris. The default Authenticator expects the security identity to expose role names in the format <code>POLARIS_ROLE:<role name></code>.  <p>Example:</p>  <pre> oidc:<br/>   principalRolesMapper:<br/>     mappings:<br/>     - regex: role_(.*)<br/>       replacement: POLARIS_ROLE:$1 </pre></td>
		</tr>
	</tbody>
</table>
<h3>CORS</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>cors.allowedOrigins</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>Origins allowed for CORS, e.g. http://polaris.apache.org, http://localhost:8181. In case an entry of the list is surrounded by forward slashes, it is interpreted as a regular expression.</td>
		</tr>
		<tr>
			<td>cors.allowedMethods</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>HTTP methods allowed for CORS, ex: GET, PUT, POST. If this is not set or empty, all requested methods are considered allowed.</td>
		</tr>
		<tr>
			<td>cors.allowedHeaders</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>HTTP headers allowed for CORS, ex: X-Custom, Content-Disposition. If this is not set or empty, all requested headers are considered allowed.</td>
		</tr>
		<tr>
			<td>cors.exposedHeaders</td>
			<td>list</td>
			<td><pre lang="json">
[]
</pre>
</td>
			<td>HTTP headers exposed to the client, ex: X-Custom, Content-Disposition. The default is an empty list.</td>
		</tr>
		<tr>
			<td>cors.accessControlMaxAge</td>
			<td>string</td>
			<td><pre lang="json">
""
</pre>
</td>
			<td>The <code>Access-Control-Max-Age</code> response header value indicating how long the results of a pre-flight request can be cached. Must be a valid duration.</td>
		</tr>
		<tr>
			<td>cors.accessControlAllowCredentials</td>
			<td>string</td>
			<td><pre lang="json">
null
</pre>
</td>
			<td>The <code>Access-Control-Allow-Credentials</code> response header. The value of this header will default to <code>true</code> if <code>allowedOrigins</code> property is set and there is a match with the precise <code>Origin</code> header.</td>
		</tr>
	</tbody>
</table>
<h3>Rate Limiter</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>rateLimiter.type</td>
			<td>string</td>
			<td><pre lang="json">
"no-op"
</pre>
</td>
			<td>The type of rate limiter filter to use. Two built-in types are supported: default and no-op.</td>
		</tr>
		<tr>
			<td>rateLimiter.tokenBucket.type</td>
			<td>string</td>
			<td><pre lang="json">
"default"
</pre>
</td>
			<td>The type of the token bucket rate limiter. Only the default type is supported out of the box.</td>
		</tr>
		<tr>
			<td>rateLimiter.tokenBucket.requestsPerSecond</td>
			<td>int</td>
			<td><pre lang="json">
9999
</pre>
</td>
			<td>The maximum number of requests (permits) per second allowed for each realm.</td>
		</tr>
	</tbody>
</table>
<h3>Tasks</h3>
<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>tasks.maxConcurrentTasks</td>
			<td>int</td>
			<td><pre lang="json">
0
</pre>
</td>
			<td>The maximum number of concurrent tasks that can be executed at the same time. If unspecified or zero, defaults to the number of available cores.</td>
		</tr>
		<tr>
			<td>tasks.maxQueuedTasks</td>
			<td>int</td>
			<td><pre lang="json">
0
</pre>
</td>
			<td>The maximum number of tasks that can be queued up for execution. If unspecified or zero, defaults to Integer.MAX_VALUE.</td>
		</tr>
	</tbody>
</table>


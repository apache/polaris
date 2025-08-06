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
package org.apache.polaris.service.legacy;

import io.quarkus.vertx.http.ManagementInterface;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class LegacyManagementEndpoints {

  public void registerLegacyManagementRoutes(
      @Observes ManagementInterface mi,
      @ConfigProperty(name = "quarkus.management.root-path") String rootPath) {
    mi.router().get("/metrics").handler(rc -> rc.reroute(rootPath + "/metrics"));
    mi.router().get("/healthcheck").handler(rc -> rc.reroute(rootPath + "/health"));
  }
}

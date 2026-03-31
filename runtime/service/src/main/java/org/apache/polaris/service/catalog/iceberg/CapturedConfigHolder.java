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
package org.apache.polaris.service.catalog.iceberg;

import jakarta.enterprise.context.RequestScoped;
import java.util.Map;
import java.util.Optional;

/**
 * A request-scoped CDI bean that holds the captured config map from the remote catalog's loadTable
 * response. Used to pass the {@code tableId} from the HTTP interception layer to the catalog
 * handler for S3 Tables ARN construction.
 *
 * <p>Thread safety: CDI {@code @RequestScoped} beans are inherently isolated per request. The
 * {@code volatile} keyword ensures visibility across threads within the same request if async
 * processing is used.
 */
@RequestScoped
public class CapturedConfigHolder {

  private volatile Map<String, String> capturedConfig;

  public void setCapturedConfig(Map<String, String> config) {
    this.capturedConfig = config;
  }

  public Optional<String> getTableId() {
    Map<String, String> config = this.capturedConfig;
    if (config != null) {
      return Optional.ofNullable(config.get("tableId"));
    }
    return Optional.empty();
  }

  public void clear() {
    this.capturedConfig = null;
  }
}

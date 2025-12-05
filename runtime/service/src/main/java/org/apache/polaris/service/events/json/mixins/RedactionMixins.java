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

package org.apache.polaris.service.events.json.mixins;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import org.apache.polaris.service.events.json.RedactingSerializer;

/**
 * Jackson mixins for redacting sensitive information from event payloads.
 *
 * <p>These mixins are applied to various Iceberg and Polaris types to prevent sensitive data from
 * being included in serialized events sent to external systems like CloudWatch.
 *
 * <p>The mixins support different redaction modes:
 *
 * <ul>
 *   <li>PARTIAL - Redact credentials and secrets, but keep metadata like locations
 *   <li>FULL - Redact all potentially sensitive fields including locations and properties
 * </ul>
 */
public class RedactionMixins {

  /**
   * Mixin for partial redaction of TableMetadata.
   *
   * <p>In PARTIAL mode, we keep the location but redact properties that may contain secrets.
   */
  public abstract static class TableMetadataPartialRedactionMixin {
    @JsonIgnore
    public abstract Map<String, String> properties();
  }

  /**
   * Mixin for full redaction of TableMetadata.
   *
   * <p>In FULL mode, we redact both location and properties.
   */
  public abstract static class TableMetadataFullRedactionMixin {
    @JsonSerialize(using = RedactingSerializer.class)
    public abstract String location();

    @JsonIgnore
    public abstract Map<String, String> properties();
  }

  /**
   * Mixin for partial redaction of ViewMetadata.
   *
   * <p>In PARTIAL mode, we keep the location but redact properties that may contain secrets.
   */
  public abstract static class ViewMetadataPartialRedactionMixin {
    @JsonIgnore
    public abstract Map<String, String> properties();
  }

  /**
   * Mixin for full redaction of ViewMetadata.
   *
   * <p>In FULL mode, we redact both location and properties.
   */
  public abstract static class ViewMetadataFullRedactionMixin {
    @JsonSerialize(using = RedactingSerializer.class)
    public abstract String location();

    @JsonIgnore
    public abstract Map<String, String> properties();
  }

  /**
   * Mixin for redacting LoadTableResponse/LoadTableResult.
   *
   * <p>Redacts the config map and storage-credentials array which contain sensitive credential
   * information.
   */
  public abstract static class LoadTableResponseRedactionMixin {
    @JsonIgnore
    public abstract Map<String, String> config();

    @JsonIgnore
    public abstract Object storageCredentials();
  }

  /**
   * Mixin for redacting LoadViewResponse/LoadViewResult.
   *
   * <p>Redacts the config map which may contain sensitive configuration.
   */
  public abstract static class LoadViewResponseRedactionMixin {
    @JsonIgnore
    public abstract Map<String, String> config();
  }

  /**
   * Mixin for redacting ConfigResponse.
   *
   * <p>Redacts both defaults and overrides maps which may contain sensitive configuration like
   * credentials, tokens, or internal endpoints.
   *
   * <p>Note: Since ConfigResponse only has defaults and overrides, ignoring both would result in
   * an empty object that Jackson can't serialize. Instead, we ignore the entire configResponse
   * field in AfterGetConfigEvent.
   */
  public abstract static class ConfigResponseRedactionMixin {
    @JsonIgnore
    public abstract Map<String, String> defaults();

    @JsonIgnore
    public abstract Map<String, String> overrides();
  }

  /**
   * Mixin for redacting AfterGetConfigEvent.
   *
   * <p>Completely omits the configResponse field which contains sensitive configuration.
   */
  public abstract static class AfterGetConfigEventRedactionMixin {
    @JsonIgnore
    public abstract Object configResponse();
  }

  /**
   * Mixin for redacting CreateNamespaceRequest.
   *
   * <p>Redacts properties that may contain secrets or sensitive configuration.
   */
  public abstract static class CreateNamespaceRequestRedactionMixin {
    @JsonIgnore
    public abstract Map<String, String> properties();
  }

  /**
   * Mixin for redacting UpdateNamespacePropertiesRequest.
   *
   * <p>Redacts property updates and removals that may contain or reference secrets.
   */
  public abstract static class UpdateNamespacePropertiesRequestRedactionMixin {
    @JsonIgnore
    public abstract Map<String, String> updates();
  }
}


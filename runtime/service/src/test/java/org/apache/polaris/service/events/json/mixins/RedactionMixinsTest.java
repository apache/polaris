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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for RedactionMixins.
 *
 * <p>This test verifies that all the mixin classes exist and can be referenced. The actual
 * redaction behavior is tested in the integration tests (AwsCloudWatchEventListenerTest) where the
 * mixins are applied to real objects and serialized.
 */
class RedactionMixinsTest {

  @Test
  void shouldVerifyTableMetadataMixinsExist() {
    // Verify that the TableMetadata mixins exist
    assertThat(RedactionMixins.TableMetadataPartialRedactionMixin.class).isNotNull();
    assertThat(RedactionMixins.TableMetadataFullRedactionMixin.class).isNotNull();
  }

  @Test
  void shouldVerifyViewMetadataMixinsExist() {
    // Verify that the ViewMetadata mixins exist
    assertThat(RedactionMixins.ViewMetadataPartialRedactionMixin.class).isNotNull();
    assertThat(RedactionMixins.ViewMetadataFullRedactionMixin.class).isNotNull();
  }

  @Test
  void shouldVerifyResponseMixinsExist() {
    // Verify that the response mixins exist
    assertThat(RedactionMixins.LoadTableResponseRedactionMixin.class).isNotNull();
    assertThat(RedactionMixins.LoadViewResponseRedactionMixin.class).isNotNull();
    assertThat(RedactionMixins.ConfigResponseRedactionMixin.class).isNotNull();
  }

  @Test
  void shouldVerifyEventMixinsExist() {
    // Verify that the event mixins exist
    assertThat(RedactionMixins.AfterGetConfigEventRedactionMixin.class).isNotNull();
  }

  @Test
  void shouldVerifyRequestMixinsExist() {
    // Verify that the request mixins exist
    assertThat(RedactionMixins.CreateNamespaceRequestRedactionMixin.class).isNotNull();
    assertThat(RedactionMixins.UpdateNamespacePropertiesRequestRedactionMixin.class).isNotNull();
  }
}


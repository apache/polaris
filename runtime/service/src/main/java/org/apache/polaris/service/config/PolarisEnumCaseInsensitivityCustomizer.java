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
package org.apache.polaris.service.config;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.jackson.ObjectMapperCustomizer;
import jakarta.inject.Singleton;

/**
 * Makes request enums and polymorphic type ids case-insensitive on input, so a client may send e.g.
 * {@code "storageType": "s3"} as well as the canonical {@code "S3"} (see <a
 * href="https://github.com/apache/polaris/issues/996">#996</a>).
 *
 * <p>Two Jackson features are needed because the storage config discriminator is both a polymorphic
 * type id and a visible enum property:
 *
 * <ul>
 *   <li>{@link MapperFeature#ACCEPT_CASE_INSENSITIVE_VALUES} — case-insensitive matching of
 *       polymorphic type ids (the {@code @JsonSubTypes} discriminator dispatch), and
 *   <li>{@link MapperFeature#ACCEPT_CASE_INSENSITIVE_ENUMS} — case-insensitive matching of enum
 *       values, which the discriminator is also deserialized into because it is declared {@code
 *       visible = true}.
 * </ul>
 *
 * <p>This only relaxes what is accepted on input; serialization still emits the canonical value and
 * unknown values are still rejected. It applies to the shared Quarkus {@link ObjectMapper}, so the
 * leniency is uniform across the API rather than special-cased per type — which is what keeps it
 * maintainable as new enums and discriminators are added.
 */
@Singleton
public class PolarisEnumCaseInsensitivityCustomizer implements ObjectMapperCustomizer {

  @Override
  public void customize(ObjectMapper objectMapper) {
    objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_VALUES, true);
    objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);
  }
}

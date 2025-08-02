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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import io.quarkus.jackson.ObjectMapperCustomizer;
import io.quarkus.runtime.configuration.MemorySize;
import io.quarkus.runtime.configuration.MemorySizeConverter;
import io.smallrye.config.WithConverter;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.iceberg.rest.RESTSerializers;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class JacksonConfig implements ObjectMapperCustomizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(JacksonConfig.class);

  private final long maxBodySize;

  @Inject
  public JacksonConfig(
      @ConfigProperty(name = "quarkus.http.limits.max-body-size")
          @WithConverter(MemorySizeConverter.class)
          MemorySize maxBodySize) {
    this.maxBodySize = maxBodySize.asLongValue();
  }

  @Override
  public void customize(ObjectMapper objectMapper) {
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
    RESTSerializers.registerAll(objectMapper);
    Serializers.registerSerializers(objectMapper);
    objectMapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder().maxDocumentLength(maxBodySize).build());
    LOGGER.info("Limiting request body size to {} bytes", maxBodySize);
  }
}

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

package org.apache.polaris.service.quarkus.entity.transformation;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import java.util.List;
import java.util.Optional;
import org.apache.polaris.core.entity.transformation.EntityTransformer;
import org.apache.polaris.service.entity.transformation.EntityTransformationConfiguration;

/**
 * Quarkus-specific configuration interface for entity transformation behavior.
 *
 * <p>This configuration determines which {@link EntityTransformer}s are applied during entity
 * transformation and in what order. Only the listed transformers will be used, and they will be
 * executed sequentially as configured.
 *
 * <p>If no transformers are specified, entity transformation is effectively disabled.
 */
@StaticInitSafe
@ConfigMapping(prefix = "polaris.entity-transformation")
public interface QuarkusEntityTransformationConfiguration
    extends EntityTransformationConfiguration {
  @Override
  Optional<List<String>> transformers();
}

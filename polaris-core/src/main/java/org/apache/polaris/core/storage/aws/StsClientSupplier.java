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

package org.apache.polaris.core.storage.aws;

import jakarta.annotation.Nullable;
import java.net.URI;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import software.amazon.awssdk.services.sts.StsClient;

public interface StsClientSupplier {

  StsClient stsClient(StsDestination destination);

  @PolarisImmutable
  interface StsDestination {
    @Value.Parameter(order = 1)
    Optional<URI> endpoint();

    @Value.Parameter(order = 2)
    Optional<String> region();

    static StsDestination of(@Nullable URI endpoint, @Nullable String region) {
      return ImmutableStsDestination.of(Optional.ofNullable(endpoint), Optional.ofNullable(region));
    }
  }
}

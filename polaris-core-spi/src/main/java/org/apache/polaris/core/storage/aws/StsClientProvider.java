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
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsBaseClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.endpoints.StsEndpointProvider;

public interface StsClientProvider {

  /**
   * Returns an STS client for the given destination (endpoint + region). The returned client may
   * not be a fresh instance for every call, however the client is reusable for multiple concurrent
   * requests from multiple threads. If the endpoint or region parameters are not specified, AWS SDK
   * defaults will be used.
   *
   * @param destination Endpoint and Region data for the client. Both values are optional.
   */
  StsClient stsClient(StsDestination destination);

  @PolarisImmutable
  interface StsDestination {
    /** Corresponds to {@link StsBaseClientBuilder#endpointProvider(StsEndpointProvider)} */
    @Value.Parameter(order = 1)
    Optional<URI> endpoint();

    /** Corresponds to {@link AwsClientBuilder#region(Region)} */
    @Value.Parameter(order = 2)
    Optional<String> region();

    static StsDestination of(@Nullable URI endpoint, @Nullable String region) {
      return ImmutableStsDestination.of(Optional.ofNullable(endpoint), Optional.ofNullable(region));
    }
  }
}

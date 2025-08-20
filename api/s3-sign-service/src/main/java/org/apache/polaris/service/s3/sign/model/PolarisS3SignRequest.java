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

package org.apache.polaris.service.s3.sign.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import org.apache.iceberg.aws.s3.signer.S3SignRequest;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * Request for S3 signing.
 *
 * <p>Copy of {@link S3SignRequest}, because the original does not have Jackson annotations.
 */
@PolarisImmutable
@JsonDeserialize(as = ImmutablePolarisS3SignRequest.class)
@JsonSerialize(as = ImmutablePolarisS3SignRequest.class)
@SuppressWarnings("immutables:subtype")
public interface PolarisS3SignRequest extends S3SignRequest {

  @Value.Default
  @Nullable // Replace javax.annotation.Nullable from S3SignRequest with jakarta.annotation.Nullable
  @Override
  default String body() {
    return null;
  }

  default boolean write() {
    return method().equalsIgnoreCase("PUT")
        || method().equalsIgnoreCase("POST")
        || method().equalsIgnoreCase("DELETE")
        || method().equalsIgnoreCase("PATCH");
  }
}

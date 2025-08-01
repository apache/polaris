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
import org.apache.iceberg.aws.s3.signer.S3SignResponse;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Response for S3 signing requests.
 *
 * <p>Copy of {@link S3SignResponse}, because the original does not have Jackson annotations.
 */
@PolarisImmutable
@JsonDeserialize(as = ImmutablePolarisS3SignResponse.class)
@JsonSerialize(as = ImmutablePolarisS3SignResponse.class)
@SuppressWarnings("immutables:subtype")
public interface PolarisS3SignResponse extends S3SignResponse {}

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

package org.apache.polaris.service.storage.s3.sign;

import org.apache.iceberg.aws.s3.signer.S3SignRequest;
import org.apache.polaris.service.s3.sign.model.PolarisS3SignResponse;

/** Interface for signing S3 requests. */
public interface S3RequestSigner {

  /** Signs an S3 request. */
  PolarisS3SignResponse signRequest(S3SignRequest signingRequest);
}

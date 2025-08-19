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

package org.apache.polaris.test.commons.s3mock;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import java.util.Map;
import org.apache.polaris.containerspec.ContainerSpecHelper;

public class S3Mock extends S3MockContainer {

  private static final String DEFAULT_BUCKETS = "my-bucket,my-old-bucket";
  private static final String DEFAULT_ACCESS_KEY = "ap1";
  private static final String DEFAULT_SECRET_KEY = "s3cr3t";

  public S3Mock() {
    this(DEFAULT_BUCKETS);
  }

  public S3Mock(String initialBuckets) {
    super(
        ContainerSpecHelper.containerSpecHelper("s3mock", S3Mock.class)
            .dockerImageName(null)
            .asCompatibleSubstituteFor("adobe/s3mock"));
    this.withInitialBuckets(initialBuckets);
  }

  public Map<String, String> getS3ConfigProperties() {
    String endpoint = this.getHttpEndpoint();
    return Map.of(
        "table-default.s3.endpoint", endpoint,
        "table-default.s3.path-style-access", "true",
        "table-default.s3.access-key-id", DEFAULT_ACCESS_KEY,
        "table-default.s3.secret-access-key", DEFAULT_SECRET_KEY,
        "s3.endpoint", endpoint,
        "s3.path-style-access", "true",
        "s3.access-key-id", DEFAULT_ACCESS_KEY,
        "s3.secret-access-key", DEFAULT_SECRET_KEY);
  }
}

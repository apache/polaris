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

package org.apache.polaris.storage.files.impl;

import static java.lang.String.format;

import java.util.Map;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.objectstoragemock.ObjectStorageMock;

public class TestFileOperationsImplWithS3 extends BaseTestFileOperationsImpl {

  @Override
  protected String bucket() {
    return "bucket";
  }

  @Override
  protected Map<String, String> icebergProperties(ObjectStorageMock.MockServer server) {
    return Map.of(
        "s3.access-key-id", "accessKey",
        "s3.secret-access-key", "secretKey",
        "s3.endpoint", server.getS3BaseUri().toString(),
        // must enforce path-style access because S3Resource has the bucket name in its path
        "s3.path-style-access", "true",
        "client.region", "eu-central-1",
        "http-client.type", "urlconnection");
  }

  @Override
  protected String prefix() {
    return format("s3://%s/", bucket());
  }

  @Override
  protected FileIO createFileIO() {
    return new S3FileIO();
  }
}

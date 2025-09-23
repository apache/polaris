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
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Disabled;
import org.projectnessie.objectstoragemock.ObjectStorageMock;

@Disabled(
    "Requires implementation of the /batch/storage/v1 endpoint in object-storage-mock. "
        + "That consumes a multipart/mixed content, which contains a series of serialized HTTP requests.")
public class TestFileOperationsImplWithGCS extends BaseTestFileOperationsImpl {

  @Override
  protected String bucket() {
    return "bucket";
  }

  @Override
  protected Map<String, String> icebergProperties(ObjectStorageMock.MockServer server) {
    String uri = server.getGcsBaseUri().toString();
    uri = uri.substring(0, uri.length() - 1);
    return Map.of(
        "gcs.project-id",
        "my-project",
        // MUST NOT end with a trailing slash, otherwise code like
        // com.google.cloud.storage.spi.v1.HttpStorageRpc.DefaultRpcBatch.submit inserts an
        // ambiguous empty path segment ("//").
        "gcs.service.host",
        uri,
        "gcs.no-auth",
        "true");
  }

  @Override
  protected String prefix() {
    return format("gs://%s/", bucket());
  }

  @Override
  protected FileIO createFileIO() {
    return new GCSFileIO();
  }
}

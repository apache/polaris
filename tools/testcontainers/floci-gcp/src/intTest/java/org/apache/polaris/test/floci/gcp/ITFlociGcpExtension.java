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

package org.apache.polaris.test.floci.gcp;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(FlociGcpExtension.class)
public class ITFlociGcpExtension {
  @FlociGcp(bucket = "mybucket", oauth2Token = "myoauth", projectId = "myproject")
  private static FlociGcpAccess flociGcpStaticField;

  @FlociGcp(bucket = "mybucket", oauth2Token = "myoauth", projectId = "myproject")
  private FlociGcpAccess flociGcpInstanceField;

  @Test
  public void smokeTest(
      @FlociGcp(bucket = "mybucket", oauth2Token = "myoauth", projectId = "myproject")
          FlociGcpAccess flociGcp)
      throws Exception {
    assertThat(flociGcp).isSameAs(flociGcpStaticField).isSameAs(flociGcpInstanceField);
    assertThat(flociGcp.localAddress()).isNotEmpty();
    assertThat(flociGcp.baseUri()).startsWith("http");
    assertThat(flociGcp.bucketUri()).extracting(URI::getScheme).isEqualTo("gs");
    assertThat(flociGcp.bucket()).isEqualTo("mybucket");
    assertThat(flociGcp.oauth2Token()).isEqualTo("myoauth");
    assertThat(flociGcp.projectId()).isEqualTo("myproject");

    assertThat(flociGcp.icebergProperties())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "io-impl",
                "org.apache.iceberg.gcp.gcs.GCSFileIO",
                "gcs.project-id",
                flociGcp.projectId(),
                "gcs.service.host",
                flociGcp.baseUri(),
                "gcs.oauth2.token",
                flociGcp.oauth2Token()));

    assertThat(flociGcp.hadoopConfig())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                "fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
                "fs.gs.storage.root.url",
                flociGcp.baseUri(),
                "fs.gs.project.id",
                flociGcp.projectId(),
                "fs.gs.auth.type",
                "USER_CREDENTIALS",
                "fs.gs.auth.client.id",
                "foo",
                "fs.gs.auth.client.secret",
                "bar",
                "fs.gs.auth.refresh.token",
                "baz"));

    byte[] data = "hello world".getBytes(UTF_8);
    try (Storage storage = flociGcp.newStorage()) {
      BlobId blobId = BlobId.of(flociGcp.bucket(), "some-key");
      assertThat(flociGcp.bucketUri(blobId.getName()))
          .isEqualTo(URI.create("gs://" + flociGcp.bucket() + "/" + blobId.getName()));

      storage.create(BlobInfo.newBuilder(blobId).setContentType("text/plain").build(), data);
      assertThat(storage.readAllBytes(blobId)).isEqualTo(data);
      assertThat(storage.get(blobId)).extracting(Blob::getContentType).isEqualTo("text/plain");
      assertThat(storage.list(flociGcp.bucket()).iterateAll())
          .anySatisfy(blob -> assertThat(blob.getName()).isEqualTo("some-key"));
      storage.delete(blobId);
      assertThat(storage.get(blobId)).isNull();

      BlobId writerBlobId = BlobId.of(flociGcp.bucket(), "writer-key");
      try (WriteChannel channel =
          storage.writer(BlobInfo.newBuilder(writerBlobId).setContentType("text/plain").build())) {
        channel.write(ByteBuffer.wrap(data));
      }
      assertThat(storage.readAllBytes(writerBlobId)).isEqualTo(data);
      storage.delete(writerBlobId);
    }
  }

  @Test
  public void injectsStaticAndInstanceFields() {
    assertThat(flociGcpStaticField).isNotNull();
    assertThat(flociGcpInstanceField).isSameAs(flociGcpStaticField);
  }

  @Test
  public void parametersReuseIdenticalConfiguration(
      @FlociGcp(bucket = "mybucket", oauth2Token = "myoauth", projectId = "myproject")
          FlociGcpAccess param1,
      @FlociGcp(bucket = "mybucket", oauth2Token = "myoauth", projectId = "myproject")
          FlociGcpAccess param2) {
    assertThat(param1).isSameAs(param2).isSameAs(flociGcpStaticField);
  }
}

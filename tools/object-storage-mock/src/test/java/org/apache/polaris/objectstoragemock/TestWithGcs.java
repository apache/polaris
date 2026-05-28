/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.polaris.objectstoragemock;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.polaris.objectstoragemock.HeapStorageBucket.newHeapStorageBucket;

import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.google.api.gax.paging.Page;
import com.google.cloud.NoCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.polaris.objectstoragemock.Bucket.ListElement;
import org.apache.polaris.objectstoragemock.ObjectStorageMock.MockServer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test {@link ObjectStorageMock} using {@link DataLakeFileSystemClientBuilder}. This test class is
 * separate from {@link TestMockServer}, because class names of the awssdk and the dto classes
 * clash.
 */
@ExtendWith(SoftAssertionsExtension.class)
public class TestWithGcs extends AbstractObjectStorageMockServer {

  public static final String MY_OBJECT_KEY = "my/object/key";
  public static final String DOES_NOT_EXIST = "does/not/exist";
  public static final String BUCKET = "bucket";

  @InjectSoftAssertions private SoftAssertions soft;

  protected Storage client;

  @Override
  protected void onCreated(MockServer serverInstance) {
    StorageOptions.Builder builder =
        StorageOptions.http()
            .setProjectId("project-id")
            .setHost(serverInstance.getGcsBaseUri().toString())
            .setCredentials(NoCredentials.getInstance());

    this.client = builder.build().getService();
  }

  @AfterEach
  public void closeClient() throws Exception {
    try {
      client.close();
    } finally {
      client = null;
    }
  }

  @Test
  public void listObjects() {

    IntFunction<String> intToKey =
        i -> String.format("%02d/%02d/%02d/%d", i / 1_000, (i / 100) % 10, (i / 10) % 10, i % 10);

    Bucket.Lister lister =
        (String prefix, String offset) ->
            IntStream.range(0, 4_000)
                .mapToObj(
                    i ->
                        new ListElement() {
                          @Override
                          public String key() {
                            return intToKey.apply(i);
                          }

                          @Override
                          public MockObject object() {
                            return MockObject.builder().etag(Integer.toString(i)).build();
                          }
                        });

    createServer(b -> b.putBuckets(BUCKET, Bucket.builder().lister(lister).build()));

    soft.assertThat(
            client
                .list(BUCKET, Storage.BlobListOption.prefix("00/00/01/"))
                .streamAll()
                .map(Blob::getName)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(
            IntStream.rangeClosed(10, 19).mapToObj(intToKey).collect(Collectors.toList()));

    soft.assertThat(
            client
                .list(BUCKET, Storage.BlobListOption.prefix("03/05/05/"))
                .streamAll()
                .map(Blob::getName)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(
            IntStream.rangeClosed(3_550, 3_559).mapToObj(intToKey).collect(Collectors.toList()));

    soft.assertThat(
            client
                .list(BUCKET, Storage.BlobListOption.prefix("02/05/"))
                .streamAll()
                .map(Blob::getName)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(
            IntStream.rangeClosed(2_500, 2_599).mapToObj(intToKey).collect(Collectors.toList()));

    Page<Blob> page =
        client.list(
            BUCKET, Storage.BlobListOption.prefix("02/05/"), Storage.BlobListOption.pageSize(13));
    List<Integer> pageSizes = new ArrayList<>();
    while (page != null) {
      pageSizes.add((int) page.streamValues().count());
      page = page.hasNextPage() ? page.getNextPage() : null;
    }
    soft.assertThat(pageSizes).containsExactly(13, 13, 13, 13, 13, 13, 13, 9);
  }

  @Test
  public void headObject() {
    Map<String, MockObject> objects = new HashMap<>();

    createServer(b -> b.putBuckets(BUCKET, Bucket.builder().object(objects::get).build()));

    MockObject obj =
        ImmutableMockObject.builder()
            .contentLength(42L)
            .etag("etagX")
            .contentType("application/foo")
            .lastModified(12345678000L)
            .build();
    objects.put(MY_OBJECT_KEY, obj);

    soft.assertThat(client.get(BlobId.of(BUCKET, DOES_NOT_EXIST))).isNull();

    soft.assertThat(client.get(BlobId.of(BUCKET, MY_OBJECT_KEY)))
        .extracting(
            Blob::getSize,
            Blob::getEtag,
            Blob::getContentType,
            h -> h.getUpdateTimeOffsetDateTime().toInstant().toEpochMilli())
        .containsExactly(obj.contentLength(), obj.etag(), obj.contentType(), obj.lastModified());
  }

  @Test
  public void deleteObject() {
    Map<String, MockObject> objects = new HashMap<>();

    createServer(
        b ->
            b.putBuckets(
                BUCKET,
                Bucket.builder()
                    .object(objects::get)
                    .deleter(o -> objects.remove(o) != null)
                    .build()));

    MockObject obj = ImmutableMockObject.builder().build();
    objects.put(MY_OBJECT_KEY, obj);

    soft.assertThat(client.delete(BlobId.of(BUCKET, DOES_NOT_EXIST))).isFalse();

    soft.assertThat(client.delete(BlobId.of(BUCKET, MY_OBJECT_KEY))).isTrue();
    soft.assertThat(objects).doesNotContainKey(MY_OBJECT_KEY);

    objects.put(MY_OBJECT_KEY, obj);
    soft.assertThat(client.delete(BlobId.of(BUCKET, MY_OBJECT_KEY))).isTrue();
  }

  @Test
  public void getObject() {
    Map<String, MockObject> objects = new HashMap<>();

    createServer(
        b ->
            b.putBuckets(
                BUCKET, Bucket.builder().object(objects::get).deleter(o -> false).build()));

    byte[] content = "Hello World\nHello Polaris!".getBytes(UTF_8);

    MockObject obj =
        ImmutableMockObject.builder()
            .contentLength(content.length)
            .contentType("text/plain")
            .writer((range, w) -> w.write(content))
            .build();
    objects.put(MY_OBJECT_KEY, obj);

    soft.assertThat(client.get(BlobId.of(BUCKET, MY_OBJECT_KEY)))
        .extracting(Blob::getContentType, Blob::getSize)
        .containsExactly("text/plain", (long) content.length);

    soft.assertThat(client.readAllBytes(BlobId.of(BUCKET, MY_OBJECT_KEY))).containsExactly(content);
  }

  @Test
  public void putObject() throws Exception {
    Bucket heap = newHeapStorageBucket().bucket();

    createServer(b -> b.putBuckets(BUCKET, heap));

    client.createFrom(
        BlobInfo.newBuilder(BlobId.of(BUCKET, MY_OBJECT_KEY)).setContentType("text/plain").build(),
        new ByteArrayInputStream("Hello World".getBytes(UTF_8)));

    soft.assertThat(heap.object().retrieve(MY_OBJECT_KEY))
        .extracting(MockObject::contentType, MockObject::contentLength)
        .containsExactly("text/plain", (long) "Hello World".length());
  }

  @Test
  public void putObjectNoContentType() throws Exception {
    Bucket heap = newHeapStorageBucket().bucket();

    createServer(b -> b.putBuckets(BUCKET, heap));

    client.createFrom(
        BlobInfo.newBuilder(BlobId.of(BUCKET, MY_OBJECT_KEY)).build(),
        new ByteArrayInputStream("Hello World".getBytes(UTF_8)));

    soft.assertThat(heap.object().retrieve(MY_OBJECT_KEY))
        .extracting(MockObject::contentType, MockObject::contentLength)
        .containsExactly("application/octet-stream", (long) "Hello World".length());
  }

  @Test
  public void putObjectWithWriter() throws Exception {
    Bucket heap = newHeapStorageBucket().bucket();

    createServer(b -> b.putBuckets(BUCKET, heap));

    try (WriteChannel channel =
        client.writer(
            BlobInfo.newBuilder(BlobId.of(BUCKET, MY_OBJECT_KEY))
                .setContentType("text/plain")
                .build())) {
      Channels.newOutputStream(channel).write("Hello World".getBytes(UTF_8));
    }

    soft.assertThat(heap.object().retrieve(MY_OBJECT_KEY))
        .extracting(MockObject::contentType, MockObject::contentLength)
        .containsExactly("text/plain", (long) "Hello World".length());
  }

  @Test
  public void heapStorage() throws Exception {
    createServer(b -> b.putBuckets(BUCKET, newHeapStorageBucket().bucket()));

    byte[] data = "Hello World".getBytes(UTF_8);
    client.createFrom(
        BlobInfo.newBuilder(BlobId.of(BUCKET, MY_OBJECT_KEY)).setContentType("text/plain").build(),
        new ByteArrayInputStream(data));

    soft.assertThat(client.get(BlobId.of(BUCKET, MY_OBJECT_KEY)))
        .extracting(Blob::getContentType, Blob::getSize)
        .containsExactly("text/plain", (long) data.length);
    soft.assertThat(client.readAllBytes(BlobId.of(BUCKET, MY_OBJECT_KEY))).containsExactly(data);

    List<String> contents =
        client.list(BUCKET).streamAll().map(Blob::getName).collect(Collectors.toList());
    soft.assertThat(contents).containsExactly(MY_OBJECT_KEY);

    for (int i = 0; i < 5; i++) {
      client.createFrom(
          BlobInfo.newBuilder(BlobId.of(BUCKET, "objs/o" + i)).setContentType("text/plain").build(),
          new ByteArrayInputStream(("Hello World " + i).getBytes(UTF_8)));
    }
    for (int i = 0; i < 5; i++) {
      client.createFrom(
          BlobInfo.newBuilder(BlobId.of(BUCKET, "foo/f" + i)).setContentType("text/plain").build(),
          new ByteArrayInputStream(("Foo " + i).getBytes(UTF_8)));
    }

    contents = client.list(BUCKET).streamAll().map(Blob::getName).collect(Collectors.toList());
    soft.assertThat(contents)
        .containsExactly(
            "foo/f0",
            "foo/f1",
            "foo/f2",
            "foo/f3",
            "foo/f4",
            MY_OBJECT_KEY,
            "objs/o0",
            "objs/o1",
            "objs/o2",
            "objs/o3",
            "objs/o4");

    contents =
        client
            .list(BUCKET, Storage.BlobListOption.prefix("objs"))
            .streamAll()
            .map(Blob::getName)
            .collect(Collectors.toList());
    soft.assertThat(contents)
        .containsExactly("objs/o0", "objs/o1", "objs/o2", "objs/o3", "objs/o4");
  }
}

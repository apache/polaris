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
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.azure.core.http.HttpClient;
import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.core.util.ConfigurationBuilder;
import com.azure.core.util.HttpClientOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import com.azure.storage.file.datalake.options.DataLakeFileOutputStreamOptions;
import com.azure.storage.file.datalake.options.FileParallelUploadOptions;
import java.io.OutputStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.polaris.objectstoragemock.Bucket.ListElement;
import org.apache.polaris.objectstoragemock.ObjectStorageMock.MockServer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test {@link ObjectStorageMock} using {@link DataLakeFileSystemClientBuilder}. This test class is
 * separate from {@link TestMockServer}, because class names of the awssdk and the dto classes
 * clash.
 */
@ExtendWith(SoftAssertionsExtension.class)
public class TestWithAdlsGen2 extends AbstractObjectStorageMockServer {

  public static final String MY_OBJECT_KEY = "my/object/key";
  public static final String DOES_NOT_EXIST = "does/not/exist";
  public static final String BUCKET = "bucket";
  public static final Set<OpenOption> OPEN_OPTIONS =
      Set.of(
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING);

  @InjectSoftAssertions private SoftAssertions soft;

  static HttpClient sharedHttpClient;

  @BeforeAll
  public static void setupHttpClient() {

    // Azure's Data Lake clients do not expose a close method for releasing resources held by
    // HttpClient. Reuse one shared instance so the test suite does not create additional client
    // resources for each test method.

    sharedHttpClient =
        HttpClient.createDefault(
            new HttpClientOptions()
                .setConfiguration(
                    new ConfigurationBuilder()
                        //
                        .build()));
  }

  private DataLakeFileSystemClient client;

  @Override
  protected void onCreated(MockServer serverInstance) {

    DataLakeFileSystemClientBuilder clientBuilder =
        new DataLakeFileSystemClientBuilder()
            .httpClient(sharedHttpClient)
            // for posterity...
            .configuration(new ConfigurationBuilder().build())
            .endpoint(serverInstance.getAdlsGen2BaseUri().toString())
            .credential(new StorageSharedKeyCredential("accessName", "accountKey"))
            .fileSystemName(BUCKET);

    this.client = clientBuilder.buildClient();
  }

  @AfterEach
  public void closeClient() {
    // Hm - no .close() on the client?!
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
                .listPaths(new ListPathsOptions().setRecursive(true).setPath("00/00/01/"), null)
                .stream()
                .map(PathItem::getName)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(
            IntStream.rangeClosed(10, 19).mapToObj(intToKey).collect(Collectors.toList()));

    soft.assertThat(
            client
                .listPaths(new ListPathsOptions().setRecursive(true).setPath("03/05/05/"), null)
                .stream()
                .map(PathItem::getName)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(
            IntStream.rangeClosed(3_550, 3_559).mapToObj(intToKey).collect(Collectors.toList()));

    soft.assertThat(
            client
                .listPaths(new ListPathsOptions().setRecursive(true).setPath("02/05/"), null)
                .stream()
                .map(PathItem::getName)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(
            IntStream.rangeClosed(2_500, 2_599).mapToObj(intToKey).collect(Collectors.toList()));

    soft.assertThat(
            client
                .listPaths(
                    new ListPathsOptions().setRecursive(true).setMaxResults(13).setPath("02/05/"),
                    null)
                .streamByPage()
                .map(page -> page.getValue().size()))
        .containsExactly(13, 13, 13, 13, 13, 13, 13, 9);
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

    soft.assertThatThrownBy(() -> client.getFileClient(DOES_NOT_EXIST).getProperties())
        .isInstanceOf(DataLakeStorageException.class)
        .asInstanceOf(type(DataLakeStorageException.class))
        .extracting(DataLakeStorageException::getErrorCode)
        .isEqualTo("PathNotFound");

    soft.assertThat(client.getFileClient(MY_OBJECT_KEY).getProperties())
        .extracting(
            PathProperties::getFileSize,
            PathProperties::getETag,
            PathProperties::getContentType,
            h -> h.getLastModified().toInstant().toEpochMilli())
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

    soft.assertThatThrownBy(() -> client.deleteFile(DOES_NOT_EXIST))
        .isInstanceOf(DataLakeStorageException.class)
        .asInstanceOf(type(DataLakeStorageException.class))
        .extracting(DataLakeStorageException::getErrorCode)
        .isEqualTo("PathNotFound");

    soft.assertThatCode(() -> client.deleteFile(MY_OBJECT_KEY)).doesNotThrowAnyException();
    soft.assertThat(objects).doesNotContainKey(MY_OBJECT_KEY);

    objects.put(MY_OBJECT_KEY, obj);
    soft.assertThat(client.deleteFileWithResponse(MY_OBJECT_KEY, null, null, null))
        .extracting(Response::getStatusCode)
        .isEqualTo(200);
  }

  @Test
  public void getObject(@TempDir Path dir) {
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

    Path file = dir.resolve("file1");
    soft.assertThat(
            client
                .getFileClient(MY_OBJECT_KEY)
                .readToFileWithResponse(
                    file.toString(), null, null, null, null, false, OPEN_OPTIONS, null, null))
        .extracting(Response::getValue)
        .extracting(PathProperties::getContentType, PathProperties::getFileSize)
        .containsExactly("text/plain", (long) content.length);

    file = dir.resolve("file2");
    soft.assertThat(
            client
                .getFileClient(MY_OBJECT_KEY)
                .readToFileWithResponse(
                    file.toString(),
                    null,
                    null,
                    null,
                    new DataLakeRequestConditions().setIfMatch(obj.etag()),
                    false,
                    OPEN_OPTIONS,
                    null,
                    null))
        .extracting(Response::getValue)
        .extracting(PathProperties::getContentType, PathProperties::getFileSize)
        .containsExactly("text/plain", (long) content.length);

    file = dir.resolve("file3");
    soft.assertThat(
            client
                .getFileClient(MY_OBJECT_KEY)
                .readToFileWithResponse(
                    file.toString(),
                    null,
                    null,
                    null,
                    new DataLakeRequestConditions().setIfMatch("no no no"),
                    false,
                    OPEN_OPTIONS,
                    null,
                    null))
        .extracting(Response::getValue)
        .extracting(PathProperties::getContentType, PathProperties::getFileSize)
        .containsExactly("text/plain", (long) content.length);

    file = dir.resolve("file4");
    soft.assertThat(
            client
                .getFileClient(MY_OBJECT_KEY)
                .readToFileWithResponse(
                    file.toString(),
                    null,
                    null,
                    null,
                    new DataLakeRequestConditions().setIfNoneMatch(obj.etag()),
                    false,
                    OPEN_OPTIONS,
                    null,
                    null))
        .extracting(Response::getValue)
        .extracting(PathProperties::getContentType, PathProperties::getFileSize)
        .containsExactly("text/plain", (long) content.length);
  }

  @Test
  public void putObject() {
    Bucket heap = newHeapStorageBucket().bucket();

    createServer(b -> b.putBuckets(BUCKET, heap));

    client
        .getFileClient(MY_OBJECT_KEY)
        .uploadWithResponse(
            new FileParallelUploadOptions(BinaryData.fromString("Hello World"))
                .setHeaders(new PathHttpHeaders().setContentType("text/plain")),
            null,
            null);

    soft.assertThat(heap.object().retrieve(MY_OBJECT_KEY))
        .extracting(MockObject::contentType, MockObject::contentLength)
        .containsExactly("text/plain", (long) "Hello World".length());
  }

  @Test
  public void putObjectNoContentType() {
    Bucket heap = newHeapStorageBucket().bucket();

    createServer(b -> b.putBuckets(BUCKET, heap));

    client
        .getFileClient(MY_OBJECT_KEY)
        .uploadWithResponse(
            new FileParallelUploadOptions(BinaryData.fromString("Hello World")), null, null);

    soft.assertThat(heap.object().retrieve(MY_OBJECT_KEY))
        .extracting(MockObject::contentType, MockObject::contentLength)
        .containsExactly("application/octet-stream", (long) "Hello World".length());
  }

  @Test
  public void putObjectUsingOutputStream() throws Exception {
    Bucket heap = newHeapStorageBucket().bucket();

    createServer(b -> b.putBuckets(BUCKET, heap));

    try (OutputStream output =
        client
            .getFileClient(MY_OBJECT_KEY)
            .getOutputStream(
                new DataLakeFileOutputStreamOptions()
                    .setHeaders(new PathHttpHeaders().setContentType("text/plain")))) {
      output.write("Hello World".getBytes(UTF_8));
    }

    soft.assertThat(heap.object().retrieve(MY_OBJECT_KEY))
        .extracting(MockObject::contentType, MockObject::contentLength)
        .containsExactly("text/plain", (long) "Hello World".length());
  }

  @Test
  public void putObjectUsingOutputStreamNoContentType() throws Exception {
    Bucket heap = newHeapStorageBucket().bucket();

    createServer(b -> b.putBuckets(BUCKET, heap));

    try (OutputStream output = client.getFileClient(MY_OBJECT_KEY).getOutputStream()) {
      output.write("Hello World".getBytes(UTF_8));
    }

    soft.assertThat(heap.object().retrieve(MY_OBJECT_KEY))
        .extracting(MockObject::contentType, MockObject::contentLength)
        .containsExactly("application/octet-stream", (long) "Hello World".length());
  }

  @Test
  public void heapStorage(@TempDir Path dir) {
    createServer(b -> b.putBuckets(BUCKET, newHeapStorageBucket().bucket()));

    BinaryData data = BinaryData.fromString("Hello World");
    client.getFileClient(MY_OBJECT_KEY).upload(data, false);

    Path file = dir.resolve("file1");
    soft.assertThat(
            client
                .getFileClient(MY_OBJECT_KEY)
                .readToFileWithResponse(
                    file.toString(), null, null, null, null, false, OPEN_OPTIONS, null, null))
        .extracting(Response::getValue)
        .extracting(PathProperties::getContentType, PathProperties::getFileSize)
        .containsExactly("application/octet-stream", data.getLength());
    soft.assertThat(file).content().isEqualTo("Hello World");

    List<String> contents =
        client.listPaths(new ListPathsOptions().setRecursive(true), null).stream()
            .map(PathItem::getName)
            .collect(Collectors.toList());
    soft.assertThat(contents).containsExactly(MY_OBJECT_KEY);

    for (int i = 0; i < 5; i++) {
      client.getFileClient("objs/o" + i).upload(BinaryData.fromString("Hello World " + i), false);
    }
    for (int i = 0; i < 5; i++) {
      client.getFileClient("foo/f" + i).upload(BinaryData.fromString("Foo " + i), false);
    }

    contents =
        client.listPaths(new ListPathsOptions().setRecursive(true), null).stream()
            .map(PathItem::getName)
            .collect(Collectors.toList());
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
        client.listPaths(new ListPathsOptions().setRecursive(true).setPath("objs"), null).stream()
            .map(PathItem::getName)
            .collect(Collectors.toList());
    soft.assertThat(contents)
        .containsExactly("objs/o0", "objs/o1", "objs/o2", "objs/o3", "objs/o4");
  }
}

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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.polaris.objectstoragemock.HeapStorageBucket.newHeapStorageBucket;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.polaris.objectstoragemock.Bucket.ListElement;
import org.apache.polaris.objectstoragemock.ObjectStorageMock.MockServer;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.endpoints.StsEndpointProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.utils.IoUtils;

/**
 * Test {@link ObjectStorageMock} using {@link S3Client}. This test class is separate from {@link
 * TestMockServer}, because class names of the awssdk and the dto classes clash.
 */
@ExtendWith(SoftAssertionsExtension.class)
public class TestWithS3Client extends AbstractObjectStorageMockServer {

  public static final String MY_OBJECT_KEY = "my/object/key";
  public static final String DOES_NOT_EXIST = "does/not/exist";
  public static final String BUCKET = "bucket";
  public static final String NOT_A_BUCKET = "not-a-bucket";
  private S3Client s3;

  @InjectSoftAssertions private SoftAssertions soft;

  private SdkHttpClient sdkClient;

  @Override
  protected void onCreated(MockServer serverInstance) {
    sdkClient = UrlConnectionHttpClient.builder().build();

    s3 =
        S3Client.builder()
            .httpClient(sdkClient)
            .applyMutation(builder -> builder.endpointOverride(serverInstance.getS3BaseUri()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("accessKey", "secretKey")))
            .build();
  }

  @AfterEach
  public void closeClient() {
    if (s3 != null) {
      try {
        s3.close();
      } finally {
        s3 = null;
      }
    }
  }

  @SuppressWarnings("resource")
  @Test
  public void assumeRole() {
    createServer(b -> {});

    URI endpoint = serverInstance.getStsEndpointURI();

    StsEndpointProvider provider =
        params -> completedFuture(Endpoint.builder().url(endpoint).build());

    StsClient stsClient =
        StsClient.builder()
            .httpClient(sdkClient)
            // .endpointOverride(serverInstance.getStsEndpointURI())
            .endpointProvider(provider)
            .region(Region.EU_CENTRAL_1)
            .build();

    AssumeRoleRequest request =
        AssumeRoleRequest.builder()
            .roleSessionName("polaris")
            .roleArn("arn-thingy")
            .policy("policy")
            .durationSeconds(1234)
            .externalId("external-id")
            .serialNumber("42")
            .overrideConfiguration(
                builder ->
                    builder.credentialsProvider(
                        () -> AwsBasicCredentials.create("accessKeyId", "secretAccessKey")))
            .build();

    AssumeRoleResponse response = stsClient.assumeRole(request);

    soft.assertThat(response).isNotNull();
    soft.assertThat(response.sourceIdentity()).isEqualTo("source-identity");
    soft.assertThat(response.credentials())
        .extracting(Credentials::accessKeyId, Credentials::secretAccessKey)
        .containsExactly("access-key-id", "secret-access-key");
  }

  @SuppressWarnings("resource")
  @Test
  public void listBuckets() {
    createServer(
        b ->
            b.putBuckets("secret", Bucket.builder().build())
                .putBuckets(BUCKET, Bucket.builder().build()));

    soft.assertThat(s3.listBuckets())
        .extracting(ListBucketsResponse::buckets)
        .asInstanceOf(
            InstanceOfAssertFactories.list(software.amazon.awssdk.services.s3.model.Bucket.class))
        .map(software.amazon.awssdk.services.s3.model.Bucket.class::cast)
        .map(software.amazon.awssdk.services.s3.model.Bucket::name)
        .containsExactlyInAnyOrder(BUCKET, "secret");

    soft.assertThatCode(() -> s3.headBucket(b -> b.bucket(BUCKET))).doesNotThrowAnyException();
    soft.assertThatCode(() -> s3.headBucket(b -> b.bucket("secret"))).doesNotThrowAnyException();
    soft.assertThatThrownBy(() -> s3.headBucket(b -> b.bucket("foo")))
        .isInstanceOf(NoSuchBucketException.class);
    soft.assertThatThrownBy(() -> s3.headBucket(b -> b.bucket("bar")))
        .isInstanceOf(NoSuchBucketException.class);
  }

  @SuppressWarnings("resource")
  @Test
  public void listObjectsV2() {

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
            s3
                .listObjectsV2Paginator(b -> b.bucket(BUCKET).maxKeys(743).prefix("00/00/01/"))
                .stream()
                .flatMap(p -> p.contents().stream())
                .map(S3Object::key))
        .containsExactlyElementsOf(
            IntStream.rangeClosed(10, 19).mapToObj(intToKey).collect(Collectors.toList()));

    soft.assertThat(
            s3
                .listObjectsV2Paginator(b -> b.bucket(BUCKET).maxKeys(743).prefix("03/05/05/"))
                .stream()
                .flatMap(p -> p.contents().stream())
                .map(S3Object::key))
        .containsExactlyElementsOf(
            IntStream.rangeClosed(3_550, 3_559).mapToObj(intToKey).collect(Collectors.toList()));

    soft.assertThat(
            s3.listObjectsV2Paginator(b -> b.bucket(BUCKET).maxKeys(743).prefix("02/05/")).stream()
                .flatMap(p -> p.contents().stream())
                .map(S3Object::key))
        .containsExactlyElementsOf(
            IntStream.rangeClosed(2_500, 2_599).mapToObj(intToKey).collect(Collectors.toList()));

    soft.assertThat(
            s3.listObjectsV2Paginator(b -> b.bucket(BUCKET).maxKeys(13).prefix("02/05/")).stream()
                .mapToInt(p -> p.contents().size()))
        .containsExactly(13, 13, 13, 13, 13, 13, 13, 9);
  }

  @SuppressWarnings("resource")
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

    soft.assertThatThrownBy(() -> s3.headObject(b -> b.bucket(NOT_A_BUCKET).key(DOES_NOT_EXIST)))
        .isInstanceOf(NoSuchKeyException.class);
    soft.assertThatThrownBy(() -> s3.headObject(b -> b.bucket(BUCKET).key(DOES_NOT_EXIST)))
        .isInstanceOf(NoSuchKeyException.class);
    soft.assertThat(s3.headObject(b -> b.bucket(BUCKET).key(MY_OBJECT_KEY)))
        .extracting(
            HeadObjectResponse::contentLength,
            HeadObjectResponse::eTag,
            HeadObjectResponse::contentType,
            r -> r.lastModified().toEpochMilli())
        .containsExactly(
            obj.contentLength(), '"' + obj.etag() + '"', obj.contentType(), obj.lastModified());
  }

  @SuppressWarnings("resource")
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

    soft.assertThatThrownBy(() -> s3.deleteObject(b -> b.bucket(NOT_A_BUCKET).key(DOES_NOT_EXIST)))
        .isInstanceOf(NoSuchBucketException.class);
    soft.assertThat(s3.deleteObject(b -> b.bucket(BUCKET).key(DOES_NOT_EXIST))).isNotNull();

    soft.assertThat(objects).containsKey(MY_OBJECT_KEY);
    soft.assertThat(s3.deleteObject(b -> b.bucket(BUCKET).key(MY_OBJECT_KEY))).isNotNull();
    soft.assertThat(objects).doesNotContainKey(MY_OBJECT_KEY);

    soft.assertThat(s3.deleteObject(b -> b.bucket(BUCKET).key(MY_OBJECT_KEY))).isNotNull();
  }

  @SuppressWarnings({"unchecked", "resource"})
  @Test
  public void batchDeleteObject() {
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

    soft.assertThatThrownBy(
            () ->
                s3.deleteObjects(
                    b ->
                        b.bucket(NOT_A_BUCKET)
                            .delete(d -> d.objects(oi -> oi.key(DOES_NOT_EXIST)))))
        .isInstanceOf(NoSuchBucketException.class);
    soft.assertThat(
            s3.deleteObjects(
                b -> b.bucket(BUCKET).delete(d -> d.objects(oi -> oi.key(DOES_NOT_EXIST)))))
        .isNotNull();

    soft.assertThat(objects).containsKey(MY_OBJECT_KEY);
    soft.assertThat(
            s3.deleteObjects(
                b -> b.bucket(BUCKET).delete(d -> d.objects(oi -> oi.key(MY_OBJECT_KEY)))))
        .isNotNull();
    soft.assertThat(objects).doesNotContainKey(MY_OBJECT_KEY);

    soft.assertThat(
            s3.deleteObjects(
                b -> b.bucket(BUCKET).delete(d -> d.objects(oi -> oi.key(MY_OBJECT_KEY)))))
        .isNotNull();
  }

  @SuppressWarnings("resource")
  @Test
  public void getObject() {
    Map<String, MockObject> objects = new HashMap<>();

    createServer(
        b ->
            b.putBuckets(
                BUCKET, Bucket.builder().object(objects::get).deleter(o -> false).build()));

    byte[] content = "Hello World\nHello Polaris!".getBytes(StandardCharsets.UTF_8);

    MockObject obj =
        ImmutableMockObject.builder()
            .contentLength(content.length)
            .contentType("text/plain")
            .writer((range, w) -> w.write(content))
            .build();
    objects.put(MY_OBJECT_KEY, obj);

    soft.assertThat(s3.getObject(b -> b.bucket(BUCKET).key(MY_OBJECT_KEY)))
        .hasBinaryContent(content)
        .asInstanceOf(type(ResponseInputStream.class))
        .extracting(ResponseInputStream::response)
        .asInstanceOf(type(GetObjectResponse.class))
        .extracting(GetObjectResponse::contentType, GetObjectResponse::contentLength)
        .containsExactly("text/plain", (long) content.length);

    soft.assertThat(s3.getObject(b -> b.bucket(BUCKET).key(MY_OBJECT_KEY).ifMatch(obj.etag())))
        .hasBinaryContent(content)
        .asInstanceOf(type(ResponseInputStream.class))
        .extracting(ResponseInputStream::response)
        .asInstanceOf(type(GetObjectResponse.class))
        .extracting(GetObjectResponse::contentType, GetObjectResponse::contentLength)
        .containsExactly("text/plain", (long) content.length);

    soft.assertThatThrownBy(
            () -> s3.getObject(b -> b.bucket(BUCKET).key(MY_OBJECT_KEY).ifMatch("no no no")))
        .isInstanceOf(S3Exception.class)
        .asInstanceOf(type(S3Exception.class))
        .extracting(S3Exception::awsErrorDetails)
        .extracting(AwsErrorDetails::errorCode)
        .isEqualTo("PreconditionFailed");
    soft.assertThatThrownBy(
            () -> s3.getObject(b -> b.bucket(BUCKET).key(MY_OBJECT_KEY).ifNoneMatch(obj.etag())))
        .isInstanceOf(S3Exception.class)
        .asInstanceOf(type(S3Exception.class))
        // HTTP RFCs mandate that HTTP/304 must not return a message-body
        .extracting(S3Exception::statusCode)
        .isEqualTo(304);
  }

  @SuppressWarnings("resource")
  @Test
  public void putObject() {
    AtomicReference<String> writtenKey = new AtomicReference<>();
    AtomicReference<String> writtenType = new AtomicReference<>();
    AtomicReference<byte[]> writtenData = new AtomicReference<>();

    createServer(
        b ->
            b.putBuckets(
                BUCKET,
                Bucket.builder()
                    .updater(
                        (key, mode) ->
                            new Bucket.ObjectUpdater() {
                              @Override
                              public Bucket.ObjectUpdater append(long position, InputStream data) {
                                try {
                                  writtenData.set(IoUtils.toByteArray(data));
                                } catch (IOException e) {
                                  throw new RuntimeException(e);
                                }
                                return this;
                              }

                              @Override
                              public Bucket.ObjectUpdater flush() {
                                return this;
                              }

                              @Override
                              public Bucket.ObjectUpdater setContentType(String contentType) {
                                writtenType.set(contentType);
                                return this;
                              }

                              @Override
                              public MockObject commit() {
                                writtenKey.set(key);
                                return MockObject.builder().contentType(writtenType.get()).build();
                              }
                            })
                    .build()));

    s3.putObject(
        PutObjectRequest.builder()
            .bucket(BUCKET)
            .key("my-object")
            .contentType("text/plain")
            .build(),
        RequestBody.fromBytes("Hello World".getBytes(StandardCharsets.UTF_8)));

    soft.assertThat(writtenKey.get()).isEqualTo("my-object");
    soft.assertThat(writtenType.get()).isEqualTo("text/plain");
    soft.assertThat(writtenData.get()).asString().isEqualTo("Hello World");
  }

  @SuppressWarnings("resource")
  @Test
  public void putObjectNoContentType() {
    AtomicReference<String> writtenKey = new AtomicReference<>();
    AtomicReference<String> writtenType = new AtomicReference<>();
    AtomicReference<byte[]> writtenData = new AtomicReference<>();

    createServer(
        b ->
            b.putBuckets(
                BUCKET,
                Bucket.builder()
                    .updater(
                        (key, mode) ->
                            new Bucket.ObjectUpdater() {
                              @Override
                              public Bucket.ObjectUpdater append(long position, InputStream data) {
                                try {
                                  writtenData.set(IoUtils.toByteArray(data));
                                } catch (IOException e) {
                                  throw new RuntimeException(e);
                                }
                                return this;
                              }

                              @Override
                              public Bucket.ObjectUpdater flush() {
                                return this;
                              }

                              @Override
                              public Bucket.ObjectUpdater setContentType(String contentType) {
                                writtenType.set(contentType);
                                return this;
                              }

                              @Override
                              public MockObject commit() {
                                writtenKey.set(key);
                                return MockObject.builder().contentType(writtenType.get()).build();
                              }
                            })
                    .build()));

    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET).key("my-object").build(),
        RequestBody.fromBytes("Hello World".getBytes(StandardCharsets.UTF_8)));

    soft.assertThat(writtenKey.get()).isEqualTo("my-object");
    soft.assertThat(writtenType.get()).isEqualTo("application/octet-stream");
    soft.assertThat(writtenData.get()).asString().isEqualTo("Hello World");
  }

  @SuppressWarnings("resource")
  @Test
  public void heapStorage() throws Exception {
    createServer(b -> b.putBuckets(BUCKET, newHeapStorageBucket().bucket()));

    s3.putObject(
        PutObjectRequest.builder()
            .bucket(BUCKET)
            .key("my-object")
            .contentType("text/plain")
            .build(),
        RequestBody.fromBytes("Hello World".getBytes(StandardCharsets.UTF_8)));

    soft.assertThat(
            IoUtils.toUtf8String(
                s3.getObject(GetObjectRequest.builder().bucket(BUCKET).key("my-object").build())))
        .isEqualTo("Hello World");

    List<S3Object> contents =
        s3.listObjects(ListObjectsRequest.builder().bucket(BUCKET).build()).contents();
    soft.assertThat(contents).extracting(S3Object::key).containsExactly("my-object");

    for (int i = 0; i < 5; i++) {
      s3.putObject(
          PutObjectRequest.builder()
              .bucket(BUCKET)
              .key("objs/o" + i)
              .contentType("text/plain")
              .build(),
          RequestBody.fromBytes(("Hello World " + i).getBytes(StandardCharsets.UTF_8)));
    }
    for (int i = 0; i < 5; i++) {
      s3.putObject(
          PutObjectRequest.builder()
              .bucket(BUCKET)
              .key("foo/f" + i)
              .contentType("text/plain")
              .build(),
          RequestBody.fromBytes(("Foo " + i).getBytes(StandardCharsets.UTF_8)));
    }

    contents = s3.listObjects(ListObjectsRequest.builder().bucket(BUCKET).build()).contents();
    soft.assertThat(contents)
        .extracting(S3Object::key)
        .containsExactly(
            "foo/f0",
            "foo/f1",
            "foo/f2",
            "foo/f3",
            "foo/f4",
            "my-object",
            "objs/o0",
            "objs/o1",
            "objs/o2",
            "objs/o3",
            "objs/o4");

    contents =
        s3.listObjects(ListObjectsRequest.builder().bucket(BUCKET).prefix("objs").build())
            .contents();
    soft.assertThat(contents)
        .extracting(S3Object::key)
        .containsExactly("objs/o0", "objs/o1", "objs/o2", "objs/o3", "objs/o4");
  }
}

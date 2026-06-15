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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.objectstoragemock.ObjectStorageMock.MockServer;
import org.apache.polaris.objectstoragemock.s3.Buckets;
import org.apache.polaris.objectstoragemock.s3.ErrorResponse;
import org.apache.polaris.objectstoragemock.s3.ListAllMyBucketsResult;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestMockServer extends AbstractObjectStorageMockServer {

  public static final String BUCKET = "bucket";
  public static final String MY_OBJECT_KEY = "my/object/key";
  public static final String MY_OBJECT_NAME = "my-object-name";

  @InjectSoftAssertions private SoftAssertions soft;

  @Test
  public void smoke() throws Exception {
    @SuppressWarnings("resource")
    JsonNode node = doGet(createServer(b -> {}).getS3BaseUri().resolve("ready"), JsonNode.class);
    soft.assertThat(node.get("ready")).isEqualTo(BooleanNode.TRUE);
  }

  @Test
  public void listBuckets() throws Exception {
    @SuppressWarnings("resource")
    MockServer server =
        createServer(
            b ->
                b.putBuckets("secret", Bucket.builder().build())
                    .putBuckets(BUCKET, Bucket.builder().build()));

    ListAllMyBucketsResult result = doGet(server.getS3BaseUri(), ListAllMyBucketsResult.class);
    soft.assertThat(result)
        .extracting(ListAllMyBucketsResult::buckets)
        .extracting(Buckets::buckets)
        .asInstanceOf(
            InstanceOfAssertFactories.list(org.apache.polaris.objectstoragemock.s3.Bucket.class))
        .map(org.apache.polaris.objectstoragemock.s3.Bucket.class::cast)
        .map(org.apache.polaris.objectstoragemock.s3.Bucket::name)
        .containsExactlyInAnyOrder(BUCKET, "secret");

    soft.assertThat(doHead(server.getS3BaseUri().resolve(BUCKET))).isEqualTo(200);
    soft.assertThat(doHead(server.getS3BaseUri().resolve("secret"))).isEqualTo(200);
    soft.assertThat(doHead(server.getS3BaseUri().resolve("foo"))).isEqualTo(404);
    soft.assertThat(doHead(server.getS3BaseUri().resolve("bar"))).isEqualTo(404);
  }

  @Test
  public void headObject() throws Exception {
    @SuppressWarnings("resource")
    MockServer server = createServer(b -> b.putBuckets(BUCKET, Bucket.builder().build()));

    soft.assertThat(doHead(server.getS3BaseUri().resolve(BUCKET + "/" + MY_OBJECT_NAME)))
        .isEqualTo(404);
    soft.assertThat(doHead(server.getS3BaseUri().resolve(BUCKET + "/" + MY_OBJECT_KEY)))
        .isEqualTo(404);
  }

  @Test
  public void getObject() throws Exception {
    Map<String, MockObject> objects = new HashMap<>();

    @SuppressWarnings("resource")
    MockServer server =
        createServer(
            b ->
                b.putBuckets(
                    BUCKET, Bucket.builder().object(objects::get).deleter(o -> false).build()));

    String content = "Hello World\nHello Polaris!";

    MockObject obj =
        ImmutableMockObject.builder()
            .contentLength(content.length())
            .contentType("text/plain")
            .writer((range, w) -> w.write(content.getBytes(UTF_8)))
            .build();
    objects.put(MY_OBJECT_KEY, obj);

    soft.assertThat(
            doGet(server.getS3BaseUri().resolve(BUCKET + "/" + MY_OBJECT_KEY), String.class))
        .isEqualTo(content);
    soft.assertThat(
            doGet(
                server.getS3BaseUri().resolve(BUCKET + "/" + MY_OBJECT_NAME), ErrorResponse.class))
        .extracting(ErrorResponse::code)
        .isEqualTo("NoSuchKey");
    soft.assertThat(
            doGetStatus(
                server.getS3BaseUri().resolve(BUCKET + "/" + MY_OBJECT_KEY),
                Collections.singletonMap("If-None-Match", obj.etag())))
        .isEqualTo(304);
  }

  public <T> T doGet(URI uri, Class<T> type) throws Exception {
    return doGet(uri, type, Collections.emptyMap());
  }

  public int doGetStatus(URI uri, Map<String, String> headers) throws Exception {
    URLConnection conn = uri.toURL().openConnection();
    headers.forEach(conn::addRequestProperty);
    conn.connect();
    return ((HttpURLConnection) conn).getResponseCode();
  }

  @SuppressWarnings("unchecked")
  public <T> T doGet(URI uri, Class<T> type, Map<String, String> headers) throws Exception {
    URLConnection conn = uri.toURL().openConnection();
    headers.forEach(conn::addRequestProperty);
    conn.connect();
    String contentType = conn.getHeaderField("Content-Type");

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    InputStream in = null;
    try {
      in = ((HttpURLConnection) conn).getErrorStream();
      if (in == null) {
        in = conn.getInputStream();
      }
      ByteStreams.copy(in, out);
    } finally {
      if (in != null) {
        in.close();
      }
    }

    if (contentType != null && contentType.endsWith("xml")) {
      return XML_MAPPER.readValue(out.toByteArray(), type);
    }
    if (contentType != null && contentType.endsWith("json")) {
      return JSON_MAPPER.readValue(out.toByteArray(), type);
    }
    if (type.isAssignableFrom(String.class)) {
      return (T) out.toString(UTF_8);
    }
    return (T) out.toByteArray();
  }

  public int doHead(URI uri) throws Exception {
    URLConnection conn = uri.toURL().openConnection();
    ((HttpURLConnection) conn).setRequestMethod("HEAD");
    conn.connect();
    return ((HttpURLConnection) conn).getResponseCode();
  }

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final ObjectMapper XML_MAPPER = new XmlMapper();
}

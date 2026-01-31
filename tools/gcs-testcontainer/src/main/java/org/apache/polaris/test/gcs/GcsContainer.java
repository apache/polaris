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
// CODE_COPIED_TO_POLARIS from Project Nessie 0.106.1
package org.apache.polaris.test.gcs;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.polaris.containerspec.ContainerSpecHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.Base58;

/**
 * Provides a test container for Google Cloud Storage, using the {@code
 * docker.io/fsouza/fake-gcs-server} image.
 *
 * <p>This container must use a static TCP port, see below. To at least mitigate the chance that two
 * concurrently running containers share the same IP:PORT combination, this implementation uses a
 * random IP in the range 127.0.0.1 to 127.255.255.254. This does not work on Windows and Mac.
 *
 * <p>It would be really nice to test GCS using the Fake GCS server and dynamic ports, but that's
 * impossible, because of the GCS upload protocol itself. GCS uploads are initiated using one HTTP
 * request, which returns a {@code Location} header, which contains the URL for the upload that the
 * client must use. Problem is that a Fake server running inside a Docker container cannot infer the
 * dynamically mapped port for this test, because the initial HTTP request does not have a {@code
 * Host} header.
 */
public class GcsContainer extends GenericContainer<GcsContainer>
    implements GcsAccess, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(GcsContainer.class);
  public static final int PORT = 4443;

  private final String localAddress;
  private final String oauth2token;
  private final String bucket;
  private final String projectId;

  public GcsContainer() {
    this(null, null, null, null);
  }

  @SuppressWarnings("resource")
  public GcsContainer(String image, String bucket, String projectId, String oauth2token) {
    super(
        ContainerSpecHelper.containerSpecHelper("fake-gcs-server", GcsContainer.class)
            .dockerImageName(image));

    ThreadLocalRandom rand = ThreadLocalRandom.current();
    boolean isMac = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("mac");
    boolean isWindows = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows");
    localAddress =
        isMac || isWindows
            ? "127.0.0.1"
            : format("127.%d.%d.%d", rand.nextInt(256), rand.nextInt(256), rand.nextInt(1, 255));
    if (oauth2token == null) {
      oauth2token = randomString("token");
    }
    if (bucket == null) {
      bucket = randomString("bucket");
    }
    if (projectId == null) {
      projectId = randomString("project");
    }
    this.oauth2token = oauth2token;
    this.bucket = bucket;
    this.projectId = projectId;

    withNetworkAliases(randomString("fake-gcs"));
    withLogConsumer(c -> LOGGER.info("[FAKE-GCS] {}", c.getUtf8StringWithoutLineEnding()));
    withCommand(
        "-scheme",
        "http",
        "-public-host",
        localAddress,
        "-log-level",
        "info",
        "-external-url",
        format("http://%s:%d/", localAddress, PORT));
    setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*server started at .*"));

    addExposedPort(PORT);

    // STATIC PORT BINDING!
    setPortBindings(singletonList(localAddress + ":" + PORT + ":" + PORT));
  }

  @Override
  public void start() {
    super.start();

    try (Storage storage = newStorage()) {
      storage.create(BucketInfo.newBuilder(bucket).build());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String baseUri() {
    return format("http://%s:%d", localAddress(), gcsPort());
  }

  @Override
  public String localAddress() {
    return localAddress;
  }

  @Override
  public String oauth2token() {
    return oauth2token;
  }

  @Override
  public String bucket() {
    return bucket;
  }

  @Override
  public String projectId() {
    return projectId;
  }

  @Override
  public Storage newStorage() {
    return StorageOptions.http()
        .setHost(baseUri())
        .setCredentials(OAuth2Credentials.create(new AccessToken(oauth2token, null)))
        .setProjectId(projectId)
        .build()
        .getService();
  }

  @Override
  public URI bucketUri() {
    return bucketUri("");
  }

  @Override
  public URI bucketUri(String key) {
    if (key.startsWith("/")) {
      key = key.substring(1);
    }
    return URI.create(format("gs://%s/%s", bucket, key));
  }

  private int gcsPort() {
    return getMappedPort(PORT);
  }

  private static String randomString(String prefix) {
    return prefix + "-" + Base58.randomString(6).toLowerCase(Locale.ROOT);
  }

  @Override
  public Map<String, String> hadoopConfig() {
    Map<String, String> r = new HashMap<>();
    // See https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md
    r.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    r.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    r.put("fs.gs.storage.root.url", baseUri());
    r.put("fs.gs.project.id", projectId);
    // TODO the docs don't mention anything about using an OAuth2 token :(
    r.put("fs.gs.auth.type", "USER_CREDENTIALS");
    r.put("fs.gs.auth.client.id", "foo");
    r.put("fs.gs.auth.client.secret", "bar");
    r.put("fs.gs.auth.refresh.token", "baz");
    return r;
  }

  @Override
  public Map<String, String> icebergProperties() {
    Map<String, String> r = new HashMap<>();
    r.put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO");
    r.put("gcs.project-id", projectId);
    r.put("gcs.service.host", baseUri());
    r.put("gcs.oauth2.token", oauth2token);
    return r;
  }

  @Override
  public void close() {
    stop();
  }
}

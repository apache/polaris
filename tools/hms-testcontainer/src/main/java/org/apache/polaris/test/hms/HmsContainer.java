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
package org.apache.polaris.test.hms;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import org.apache.polaris.containerspec.ContainerSpecHelper;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * Test container that starts an Apache Hive Metastore (HMS) in standalone mode, backed by an
 * embedded Derby database.
 *
 * <p>The container exposes only the Thrift endpoint (port 9083). The base apache/hive image is
 * extended at build time with hadoop-aws + aws-java-sdk-bundle (see {@code Dockerfile-hms-version})
 * so that HMS can recognize {@code s3a://} URIs when validating table locations. Use {@link
 * #withS3aEndpoint} to point HMS at an S3-compatible test backend (e.g. RustFS / MinIO running in
 * another container) so location validation succeeds end-to-end.
 */
public final class HmsContainer extends GenericContainer<HmsContainer> implements AutoCloseable {

  private static final int THRIFT_PORT = 9083;
  private static final String DOCKERFILE_RESOURCE = "Dockerfile-hms-version";

  private String hostPort;
  private String thriftUri;
  private String s3aEndpoint;
  private String s3aAccessKey;
  private String s3aSecretKey;

  @SuppressWarnings("resource")
  public HmsContainer(String image) {
    super(buildImage(image));
    withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(HmsContainer.class)));
    addExposedPort(THRIFT_PORT);
    // The apache/hive image dispatches on this env var:
    //   metastore  -> launches HiveMetaStore (Thrift on 9083)
    //   hiveserver2 -> launches HiveServer2
    withEnv("SERVICE_NAME", "metastore");
    // Wait until the Thrift port inside the container is listening. The apache/hive
    // entrypoint runs schema initialization (Derby) before launching the metastore, which
    // can take ~30s on a cold start.
    setWaitStrategy(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)));
  }

  public HmsContainer() {
    this(null);
  }

  /**
   * Configures HMS with credentials and an endpoint for S3-compatible storage so HMS can validate
   * {@code s3a://} table locations during {@code create_table}. Must be called before {@link
   * #start()}.
   */
  public HmsContainer withS3aEndpoint(String endpoint, String accessKey, String secretKey) {
    this.s3aEndpoint = endpoint;
    this.s3aAccessKey = accessKey;
    this.s3aSecretKey = secretKey;
    return this;
  }

  private static ImageFromDockerfile buildImage(String image) {
    DockerImageName base =
        ContainerSpecHelper.containerSpecHelper("hms", HmsContainer.class).dockerImageName(image);
    String dockerfile =
        readDockerfile().replaceFirst("(?m)^FROM .*$", "FROM " + base.asCanonicalNameString());
    return new ImageFromDockerfile("polaris-hms-with-aws", false)
        .withFileFromString("Dockerfile", dockerfile);
  }

  private static String readDockerfile() {
    try (InputStream in = HmsContainer.class.getResourceAsStream(DOCKERFILE_RESOURCE)) {
      Objects.requireNonNull(in, DOCKERFILE_RESOURCE + " not found on classpath");
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public String hostPort() {
    Preconditions.checkState(hostPort != null, "Container not yet started");
    return hostPort;
  }

  public String thriftUri() {
    Preconditions.checkState(thriftUri != null, "Container not yet started");
    return thriftUri;
  }

  @Override
  public void start() {
    if (s3aEndpoint != null) {
      withCopyToContainer(
          Transferable.of(buildHiveSiteXml(), 0644), "/opt/hive/conf/hive-site.xml");
    }
    super.start();
    this.hostPort = getHost() + ":" + getMappedPort(THRIFT_PORT);
    this.thriftUri = "thrift://" + this.hostPort;
  }

  private String buildHiveSiteXml() {
    return "<?xml version=\"1.0\"?>\n"
        + "<configuration>\n"
        + property("fs.s3a.endpoint", s3aEndpoint)
        + property("fs.s3a.access.key", s3aAccessKey)
        + property("fs.s3a.secret.key", s3aSecretKey)
        + property("fs.s3a.path.style.access", "true")
        + property("fs.s3a.connection.ssl.enabled", "false")
        // SimpleAWSCredentialsProvider reads the access/secret directly from the keys above.
        + property(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        + "</configuration>\n";
  }

  private static String property(String name, String value) {
    return "  <property><name>" + name + "</name><value>" + value + "</value></property>\n";
  }

  @Override
  public void close() {
    stop();
  }
}

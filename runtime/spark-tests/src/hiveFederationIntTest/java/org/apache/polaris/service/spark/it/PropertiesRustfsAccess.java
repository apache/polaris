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
package org.apache.polaris.service.spark.it;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.polaris.test.rustfs.RustfsAccess;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Test-side view of the RustFS container owned by {@link SparkTestsStartupAction}. */
final class PropertiesRustfsAccess implements RustfsAccess {
  private final Properties properties;

  private PropertiesRustfsAccess(Properties properties) {
    this.properties = properties;
  }

  static PropertiesRustfsAccess from(Path path) {
    Properties properties = new Properties();
    try (InputStream input = Files.newInputStream(path)) {
      properties.load(input);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load RustFS properties from " + path, e);
    }
    return new PropertiesRustfsAccess(properties);
  }

  @Override
  public String hostPort() {
    return value("hostPort");
  }

  @Override
  public String accessKey() {
    return value("accessKey");
  }

  @Override
  public String secretKey() {
    return value("secretKey");
  }

  @Override
  public String bucket() {
    return value("bucket");
  }

  @Override
  public Optional<String> region() {
    return Optional.ofNullable(properties.getProperty("region"));
  }

  @Override
  public String s3endpoint() {
    return value("s3endpoint");
  }

  @Override
  public S3Client s3Client() {
    return S3Client.builder()
        .httpClientBuilder(UrlConnectionHttpClient.builder())
        .endpointOverride(URI.create(s3endpoint()))
        .applyMutation(builder -> region().ifPresent(region -> builder.region(Region.of(region))))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey(), secretKey())))
        .build();
  }

  @Override
  public Map<String, String> icebergProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("s3.access-key-id", accessKey());
    props.put("s3.secret-access-key", secretKey());
    props.put("s3.endpoint", s3endpoint());
    props.put("http-client.type", "urlconnection");
    region().ifPresent(region -> props.put("client.region", region));
    return props;
  }

  @Override
  public Map<String, String> hadoopConfig() {
    Map<String, String> props = new HashMap<>();
    props.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    props.put("fs.s3a.access.key", accessKey());
    props.put("fs.s3a.secret.key", secretKey());
    props.put("fs.s3a.endpoint", s3endpoint());
    return props;
  }

  @Override
  public URI s3BucketUri(String path) {
    return URI.create(String.format("s3://%s/", bucket())).resolve(path);
  }

  private String value(String name) {
    String value = properties.getProperty(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing RustFS property: " + name);
    }
    return value;
  }
}

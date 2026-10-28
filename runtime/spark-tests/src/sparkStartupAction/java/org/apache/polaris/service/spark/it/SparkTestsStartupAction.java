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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupAction;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupContext;
import org.apache.polaris.test.rustfs.RustfsContainer;

/** Starts services required before the custom Spark-test Polaris server process starts. */
public class SparkTestsStartupAction implements PolarisServerStartupAction {
  private static final String RUSTFS_ACCESS_KEY = "test-ak-123-hive-federation";
  private static final String RUSTFS_SECRET_KEY = "test-sk-123-hive-federation";

  private RustfsContainer hiveRustfs;

  @Override
  public void start(PolarisServerStartupContext context) throws IOException {
    hiveRustfs =
        new RustfsContainer(null, RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, null, "us-west-2")
            .withStartupAttempts(5);
    hiveRustfs.start();

    context.getSystemProperties().put("polaris.s3.endpoint", hiveRustfs.s3endpoint());
    context.getSystemProperties().put("polaris.s3.access-key", hiveRustfs.accessKey());
    context.getSystemProperties().put("polaris.s3.secret-key", hiveRustfs.secretKey());

    String propertiesFile = context.getParameters().get("hiveRustfsProperties");
    if (propertiesFile != null) {
      writeHiveRustfsProperties(Path.of(propertiesFile));
    }
  }

  private void writeHiveRustfsProperties(Path path) throws IOException {
    Files.createDirectories(path.getParent());
    Properties properties = new Properties();
    properties.setProperty("hostPort", hiveRustfs.hostPort());
    properties.setProperty("accessKey", hiveRustfs.accessKey());
    properties.setProperty("secretKey", hiveRustfs.secretKey());
    properties.setProperty("bucket", hiveRustfs.bucket());
    properties.setProperty("s3endpoint", hiveRustfs.s3endpoint());
    hiveRustfs.region().ifPresent(region -> properties.setProperty("region", region));
    try (OutputStream output = Files.newOutputStream(path)) {
      properties.store(output, "RustFS instance started by SparkTestsStartupAction");
    }
  }

  @Override
  public void close() {
    if (hiveRustfs != null) {
      hiveRustfs.close();
      hiveRustfs = null;
    }
  }
}

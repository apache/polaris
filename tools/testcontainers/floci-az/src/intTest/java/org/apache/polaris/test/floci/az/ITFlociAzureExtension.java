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

package org.apache.polaris.test.floci.az;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(FlociAzureExtension.class)
public class ITFlociAzureExtension {
  @FlociAzure(storageContainer = "mycontainer")
  private static FlociAzureAccess flociAzureStaticField;

  @FlociAzure(storageContainer = "mycontainer")
  private FlociAzureAccess flociAzureInstanceField;

  @Test
  public void smokeTest(@FlociAzure(storageContainer = "mycontainer") FlociAzureAccess flociAzure)
      throws Exception {
    assertThat(flociAzure).isSameAs(flociAzureStaticField).isSameAs(flociAzureInstanceField);
    assertThat(flociAzure.endpointHostPort()).isNotEmpty();
    assertThat(flociAzure.endpoint()).startsWith("http");
    assertThat(flociAzure.storageContainer()).isEqualTo("mycontainer");
    assertThat(flociAzure.account()).isNotEmpty();
    assertThat(flociAzure.accountFq()).isEqualTo(flociAzure.account() + ".dfs.core.windows.net");
    assertThat(flociAzure.secret()).isNotEmpty();
    assertThat(flociAzure.secretBase64()).isNotEmpty();
    assertThat(flociAzure.credential()).isNotNull();

    assertThat(flociAzure.icebergProperties())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "io-impl",
                "org.apache.iceberg.azure.adlsv2.ADLSFileIO",
                "adls.connection-string." + flociAzure.accountFq(),
                flociAzure.endpoint(),
                "adls.auth.shared-key.account.name",
                flociAzure.account(),
                "adls.auth.shared-key.account.key",
                flociAzure.secretBase64()));

    assertThat(flociAzure.hadoopConfig())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "fs.azure.impl",
                "org.apache.hadoop.fs.azure.AzureNativeFileSystemStore",
                "fs.AbstractFileSystem.azure.impl",
                "org.apache.hadoop.fs.azurebfs.Abfs",
                "fs.azure.always.use.https",
                "false",
                "fs.azure.abfs.endpoint",
                flociAzure.endpointHostPort(),
                "fs.azure.test.emulator",
                "true",
                "fs.azure.storage.emulator.account.name",
                flociAzure.account(),
                "fs.azure.account.auth.type",
                "SharedKey",
                "fs.azure.account.key." + flociAzure.accountFq(),
                flociAzure.secretBase64()));

    byte[] data = "hello world".getBytes(UTF_8);
    String key = "some-key";
    assertThat(flociAzure.location(key))
        .isEqualTo(
            "abfs://" + flociAzure.storageContainer() + "@" + flociAzure.accountFq() + "/" + key);

    DataLakeFileSystemClient fileSystem =
        flociAzure.serviceClient().getFileSystemClient(flociAzure.storageContainer());
    DataLakeFileClient fileClient = fileSystem.getFileClient(key);
    try (OutputStream output = fileClient.getOutputStream()) {
      output.write(data);
    }

    try (InputStream input = fileClient.openInputStream().getInputStream()) {
      assertThat(input.readAllBytes()).isEqualTo(data);
    }
    fileClient.delete();
    assertThat(fileClient.exists()).isFalse();
  }

  @Test
  public void injectsStaticAndInstanceFields() {
    assertThat(flociAzureStaticField).isNotNull();
    assertThat(flociAzureInstanceField).isSameAs(flociAzureStaticField);
  }

  @Test
  public void parametersReuseIdenticalConfiguration(
      @FlociAzure(storageContainer = "mycontainer") FlociAzureAccess param1,
      @FlociAzure(storageContainer = "mycontainer") FlociAzureAccess param2) {
    assertThat(param1).isSameAs(param2).isSameAs(flociAzureStaticField);
  }

  @Disabled("Floci-AZ 0.9.0 returns 501 NotImplemented for DataLakeFileSystemClient.listPaths()")
  @Test
  public void listPaths(@FlociAzure(storageContainer = "listpaths") FlociAzureAccess flociAzure) {
    DataLakeFileSystemClient fileSystem =
        flociAzure.serviceClient().getFileSystemClient(flociAzure.storageContainer());
    DataLakeFileClient fileClient = fileSystem.getFileClient("some-key");
    try (OutputStream output = fileClient.getOutputStream()) {
      output.write("hello world".getBytes(UTF_8));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    assertThat(fileSystem.listPaths())
        .anySatisfy(pathItem -> assertThat(pathItem.getName()).isEqualTo("some-key"));
  }
}

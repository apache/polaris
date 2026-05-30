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
package org.projectnessie.testing.azurite;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import java.io.InputStream;
import java.io.OutputStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({AzuriteExtension.class, SoftAssertionsExtension.class})
public class ITAzuriteExtension {
  @InjectSoftAssertions private SoftAssertions soft;

  @Test
  public void smokeTest(
      @Azurite(account = "myaccesskey", secret = "mysecretkey", storageContainer = "mybucket")
          AzuriteAccess azurite)
      throws Exception {
    soft.assertThat(azurite.endpointHostPort()).isNotEmpty();
    soft.assertThat(azurite.endpoint()).isNotEmpty().startsWith("http");
    soft.assertThat(azurite.endpoint()).isNotEmpty().startsWith("http");

    soft.assertThat(azurite.storageContainer()).isNotEmpty().isEqualTo("mybucket");

    soft.assertThat(azurite.icebergProperties())
        .containsEntry("io-impl", "org.apache.iceberg.azure.adlsv2.ADLSFileIO")
        .containsEntry("adls.connection-string." + azurite.accountFq(), azurite.endpoint())
        .containsEntry("adls.auth.shared-key.account.name", azurite.account())
        .containsEntry("adls.auth.shared-key.account.key", azurite.secretBase64());

    soft.assertThat(azurite.hadoopConfig())
        .isNotNull()
        .containsEntry("fs.azure.impl", "org.apache.hadoop.fs.azure.AzureNativeFileSystemStore")
        .containsEntry("fs.AbstractFileSystem.azure.impl", "org.apache.hadoop.fs.azurebfs.Abfs")
        .containsEntry("fs.azure.always.use.https", "false")
        .containsEntry("fs.azure.abfs.endpoint", azurite.endpointHostPort())
        .containsEntry("fs.azure.account.auth.type", "SharedKey")
        .containsEntry("fs.azure.storage.emulator.account.name", azurite.account())
        .containsEntry("fs.azure.account.key." + azurite.accountFq(), azurite.secretBase64());

    DataLakeServiceClient client = azurite.serviceClient();
    byte[] data = "hello world".getBytes(UTF_8);
    String key = "some-key";

    soft.assertThat(azurite.location("some-key"))
        .isEqualTo("abfs://" + azurite.storageContainer() + "@" + azurite.accountFq() + "/" + key);

    DataLakeFileClient fileClient =
        client.getFileSystemClient(azurite.storageContainer()).getFileClient(key);
    try (OutputStream output = fileClient.getOutputStream()) {
      output.write(data);
    }

    try (InputStream input = fileClient.openInputStream().getInputStream()) {
      soft.assertThat(input.readAllBytes()).isEqualTo(data);
    }
  }
}

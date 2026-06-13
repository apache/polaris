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

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import org.apache.polaris.containerspec.ContainerSpecHelper;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.Base58;

public class AzuriteContainer extends GenericContainer<AzuriteContainer>
    implements AzuriteAccess, AutoCloseable {

  private static final int DEFAULT_PORT = 10000; // default blob service port
  private static final String LOG_WAIT_REGEX =
      "Azurite Blob service is successfully listening at .*";

  private final String storageContainer;
  private final String account;
  private final String accountFq;
  private final String secret;
  private final String secretBase64;

  public AzuriteContainer() {
    this(null, null, null, null);
  }

  public AzuriteContainer(String image, String storageContainer, String account, String secret) {
    super(
        ContainerSpecHelper.containerSpecHelper("azurite", AzuriteContainer.class)
            .dockerImageName(image));
    if (storageContainer == null) {
      storageContainer = randomString("filesystem");
    }
    if (account == null) {
      account = randomString("account");
    }
    if (secret == null) {
      secret = randomString("secret");
    }
    this.storageContainer = storageContainer;
    this.account = account;
    this.accountFq = account + ".dfs.core.windows.net";
    this.secret = secret;
    this.secretBase64 = new String(Base64.getEncoder().encode(secret.getBytes(UTF_8)), UTF_8);

    this.addExposedPort(DEFAULT_PORT);
    this.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(LOG_WAIT_REGEX));
    this.addEnv("AZURITE_ACCOUNTS", account + ":" + this.secretBase64);
    // --skipApiVersionCheck is necessary until Azurite supports the latest API version used by
    // Azure SDK 1.3.4.
    // See https://github.com/Azure/Azurite/issues/2623
    this.setCommand(
        "azurite",
        "--blobHost",
        "0.0.0.0",
        "--tableHost",
        "0.0.0.0",
        "--queueHost",
        "0.0.0.0",
        "--skipApiVersionCheck");
  }

  @Override
  public void start() {
    super.start();

    createStorageContainer();
  }

  @Override
  public void createStorageContainer() {
    serviceClient().createFileSystem(storageContainer);
  }

  @Override
  public void deleteStorageContainer() {
    serviceClient().deleteFileSystem(storageContainer);
  }

  @Override
  public DataLakeServiceClient serviceClient() {
    return new DataLakeServiceClientBuilder()
        .endpoint(endpoint())
        .credential(credential())
        .buildClient();
  }

  @Override
  public String storageContainer() {
    return storageContainer;
  }

  @Override
  public String location(String path) {
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return String.format("abfs://%s@%s/%s", storageContainer, accountFq, path);
  }

  @Override
  public String endpoint() {
    return String.format("http://%s/%s", endpointHostPort(), account);
  }

  @Override
  public String endpointHostPort() {
    return String.format("%s:%d", getHost(), getMappedPort(DEFAULT_PORT));
  }

  @Override
  public StorageSharedKeyCredential credential() {
    return new StorageSharedKeyCredential(account, secretBase64);
  }

  @Override
  public String account() {
    return account;
  }

  @Override
  public String accountFq() {
    return accountFq;
  }

  @Override
  public String secret() {
    return secret;
  }

  @Override
  public String secretBase64() {
    return secretBase64;
  }

  @Override
  public Map<String, String> icebergProperties() {
    return Map.ofEntries(
        Map.entry("io-impl", "org.apache.iceberg.azure.adlsv2.ADLSFileIO"),
        Map.entry("adls.connection-string." + accountFq, endpoint()),
        Map.entry("adls.auth.shared-key.account.name", account),
        Map.entry("adls.auth.shared-key.account.key", secretBase64));
  }

  @Override
  public Map<String, String> hadoopConfig() {
    return Map.ofEntries(
        Map.entry("fs.azure.impl", "org.apache.hadoop.fs.azure.AzureNativeFileSystemStore"),
        Map.entry("fs.AbstractFileSystem.azure.impl", "org.apache.hadoop.fs.azurebfs.Abfs"),
        Map.entry("fs.azure.always.use.https", "false"),
        Map.entry("fs.azure.abfs.endpoint", endpointHostPort()),
        Map.entry("fs.azure.test.emulator", "true"),
        Map.entry("fs.azure.storage.emulator.account.name", account),
        Map.entry("fs.azure.account.auth.type", "SharedKey"),
        Map.entry("fs.azure.account.key." + accountFq, secretBase64));
  }

  private static String randomString(String prefix) {
    return prefix + "-" + Base58.randomString(6).toLowerCase(Locale.ROOT);
  }

  @Override
  public void close() {
    stop();
  }
}

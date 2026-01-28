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
package org.apache.polaris.test.azurite;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import java.util.Base64;
import java.util.HashMap;
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
    Map<String, String> r = new HashMap<>();
    r.put("io-impl", "org.apache.iceberg.azure.adlsv2.ADLSFileIO");
    r.put("adls.connection-string." + accountFq, endpoint());
    r.put("adls.auth.shared-key.account.name", account);
    r.put("adls.auth.shared-key.account.key", secretBase64);
    return r;
  }

  @Override
  public Map<String, String> hadoopConfig() {
    Map<String, String> r = new HashMap<>();

    r.put("fs.azure.impl", "org.apache.hadoop.fs.azure.AzureNativeFileSystemStore");
    r.put("fs.AbstractFileSystem.azure.impl", "org.apache.hadoop.fs.azurebfs.Abfs");

    r.put("fs.azure.always.use.https", "false");
    r.put("fs.azure.abfs.endpoint", endpointHostPort());

    r.put("fs.azure.test.emulator", "true");
    r.put("fs.azure.storage.emulator.account.name", account);
    r.put("fs.azure.account.auth.type", "SharedKey");
    r.put("fs.azure.account.key." + accountFq, secretBase64);

    return r;
  }

  private static String randomString(String prefix) {
    return prefix + "-" + Base58.randomString(6).toLowerCase(Locale.ROOT);
  }

  @Override
  public void close() {
    stop();
  }
}

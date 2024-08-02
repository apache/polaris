/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.core.storage.azure;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jetbrains.annotations.NotNull;

/** This class represents all information for a azure location */
public class AzureLocation {
  /** The pattern only allows abfs[s] now because the ResovlingFileIO only accept ADLSFileIO */
  private static final Pattern URI_PATTERN = Pattern.compile("^abfss?://([^/?#]+)(.*)?$");

  public static final String ADLS_ENDPOINT = "dfs.core.windows.net";

  public static final String BLOB_ENDPOINT = "blob.core.windows.net";

  private final String storageAccount;
  private final String container;

  private final String endpoint;
  private final String filePath;

  /**
   * Construct an Azure location object from a location uri, it should follow this pattern:
   *
   * <pre> abfs[s]://[<container>@]<storage account host>/<file path> </pre>
   *
   * @param location a uri
   */
  public AzureLocation(@NotNull String location) {
    Matcher matcher = URI_PATTERN.matcher(location);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid azure adls location uri " + location);
    }
    String authority = matcher.group(1);
    // look for <container>@<storage account host>
    String[] parts = authority.split("@", -1);

    // expect container and account both exist
    if (parts.length != 2) {
      throw new IllegalArgumentException("container and account name must be both provided");
    }
    this.container = parts[0];
    String accountHost = parts[1];
    String[] hostParts = accountHost.split("\\.", 2);
    if (hostParts.length != 2) {
      throw new IllegalArgumentException("storage account and endpoint must be both provided");
    }
    this.storageAccount = hostParts[0];
    this.endpoint = hostParts[1];
    String path = matcher.group(2);
    filePath = path == null ? "" : path.startsWith("/") ? path.substring(1) : path;
  }

  /** Get the storage account */
  public String getStorageAccount() {
    return storageAccount;
  }

  /** Get the container name */
  public String getContainer() {
    return container;
  }

  /** Get the endpoint, for example: blob.core.windows.net */
  public String getEndpoint() {
    return endpoint;
  }

  /** Get the file path */
  public String getFilePath() {
    return filePath;
  }
}

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
package org.apache.polaris.core.storage.azure;

import jakarta.annotation.Nonnull;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.polaris.core.storage.StorageLocation;

/** This class represents all information for a azure location */
public class AzureLocation extends StorageLocation {
  private static final Pattern URI_PATTERN = Pattern.compile("^(abfss?|wasbs?)://([^/?#]+)(.*)?$");

  public static final String ADLS_ENDPOINT = "dfs.core.windows.net";

  public static final String BLOB_ENDPOINT = "blob.core.windows.net";

  private final String scheme;
  private final String storageAccount;
  private final String container;

  private final String endpoint;
  private final String filePath;

  /**
   * Construct an Azure location object from a location uri, it should follow this pattern:
   *
   * <p>{@code (abfs|wasb)[s]://[<container>@]<storage account host>/<file path>}
   *
   * @param location a uri
   */
  public AzureLocation(@Nonnull String location) {
    super(location);

    Matcher matcher = URI_PATTERN.matcher(location);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid azure location uri " + location);
    }
    this.scheme = matcher.group(1);
    String authority = matcher.group(2);
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
    String path = matcher.group(3);
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

  /** Get the scheme */
  public String getScheme() {
    return scheme;
  }

  /**
   * Returns true if the object this StorageLocation refers to is a child of the object referred to
   * by the other StorageLocation.
   */
  @Override
  public boolean isChildOf(@Nonnull StorageLocation potentialParent) {
    if (potentialParent instanceof AzureLocation) {
      AzureLocation potentialAzureParent = (AzureLocation) potentialParent;
      if (this.container.equals(potentialAzureParent.container)) {
        if (this.storageAccount.equals(potentialAzureParent.storageAccount)) {
          String formattedFilePath = ensureLeadingSlash(ensureTrailingSlash(this.filePath));
          String formattedParentFilePath =
              ensureLeadingSlash(ensureTrailingSlash(potentialAzureParent.filePath));
          return formattedFilePath.startsWith(formattedParentFilePath);
        }
      }
    }
    return false;
  }

  /** Return true if the input location appears to be an Azure path */
  public static boolean isAzureLocation(String location) {
    if (location == null) {
      return false;
    }
    Matcher matcher = URI_PATTERN.matcher(location);
    return matcher.matches();
  }
}

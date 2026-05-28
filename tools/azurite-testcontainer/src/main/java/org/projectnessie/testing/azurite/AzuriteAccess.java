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

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import java.util.Map;

public interface AzuriteAccess {
  DataLakeServiceClient serviceClient();

  String storageContainer();

  String location(String path);

  String endpoint();

  String endpointHostPort();

  String account();

  String accountFq();

  String secret();

  String secretBase64();

  StorageSharedKeyCredential credential();

  Map<String, String> icebergProperties();

  Map<String, String> hadoopConfig();

  void createStorageContainer();

  void deleteStorageContainer();
}

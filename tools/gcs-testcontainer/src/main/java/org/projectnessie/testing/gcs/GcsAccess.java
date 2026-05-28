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
package org.projectnessie.testing.gcs;

import com.google.cloud.storage.Storage;
import java.net.URI;
import java.util.Map;

public interface GcsAccess {
  String baseUri();

  String localAddress();

  String oauth2token();

  String bucket();

  URI bucketUri();

  String projectId();

  Storage newStorage();

  Map<String, String> hadoopConfig();

  Map<String, String> icebergProperties();

  URI bucketUri(String key);
}

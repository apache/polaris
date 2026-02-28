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

package org.apache.polaris.storage.files.impl;

import java.util.Map;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.testing.azurite.Azurite;
import org.projectnessie.testing.azurite.AzuriteAccess;
import org.projectnessie.testing.azurite.AzuriteExtension;

@ExtendWith(AzuriteExtension.class)
public class ITFileOperationsImplWithADLS extends BaseITFileOperationsImpl {

  @Azurite static AzuriteAccess azuriteAccess;

  @Override
  protected Map<String, String> icebergProperties() {
    return azuriteAccess.icebergProperties();
  }

  @Override
  protected String prefix() {
    return azuriteAccess.location("");
  }

  @Override
  protected FileIO createFileIO() {
    return new ADLSFileIO();
  }

  @Override
  @Disabled("Azurite is incompatible with ADLS v2 list-prefix REST endpoint")
  public void icebergIntegration() throws Exception {
    super.icebergIntegration();
  }
}

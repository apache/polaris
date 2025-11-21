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
package org.apache.polaris.service.catalog.io;

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.storage.StorageAccessConfig;

/** A {@link FileIOFactory} that translates WASB paths to ABFS ones */
@ApplicationScoped
@Identifier("wasb")
public class WasbTranslatingFileIOFactory implements FileIOFactory {

  private final FileIOFactory defaultFileIOFactory;

  @Inject
  public WasbTranslatingFileIOFactory() {
    defaultFileIOFactory = new DefaultFileIOFactory();
  }

  @Override
  public FileIO loadFileIO(
      @Nonnull StorageAccessConfig accessConfig,
      @Nonnull String ioImplClassName,
      @Nonnull Map<String, String> properties) {
    return new WasbTranslatingFileIO(
        defaultFileIOFactory.loadFileIO(accessConfig, ioImplClassName, properties));
  }
}

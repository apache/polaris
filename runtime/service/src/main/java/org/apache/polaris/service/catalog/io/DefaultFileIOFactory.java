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

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.storage.AccessConfig;

/**
 * A default FileIO factory implementation for creating Iceberg {@link FileIO} instances with
 * contextual table-level properties.
 *
 * <p>This class acts as a translation layer between Polaris properties and the properties required
 * by Iceberg's {@link FileIO}.
 */
@ApplicationScoped
@Identifier("default")
public class DefaultFileIOFactory implements FileIOFactory {

  @Inject
  public DefaultFileIOFactory() {}

  @Override
  public FileIO loadFileIO(
      @Nonnull AccessConfig accessConfig,
      @Nonnull String ioImplClassName,
      @Nonnull Map<String, String> properties) {

    // Get subcoped creds
    properties = new HashMap<>(properties);

    // Update the FileIO with the subscoped credentials
    // Update with properties in case there are table-level overrides the credentials should
    // always override table-level properties, since storage configuration will be found at
    // whatever entity defines it
    properties.putAll(accessConfig.credentials());
    properties.putAll(accessConfig.extraProperties());
    properties.putAll(accessConfig.internalProperties());

    return loadFileIOInternal(ioImplClassName, properties);
  }

  @VisibleForTesting
  FileIO loadFileIOInternal(
      @Nonnull String ioImplClassName, @Nonnull Map<String, String> properties) {
    return new ExceptionMappingFileIO(CatalogUtil.loadFileIO(ioImplClassName, properties, null));
  }
}

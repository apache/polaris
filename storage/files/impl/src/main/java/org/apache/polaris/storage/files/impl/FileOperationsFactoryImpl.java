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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.storage.files.api.FileOperations;
import org.apache.polaris.storage.files.api.FileOperationsFactory;

/** CDI application-scoped implementation of {@link FileOperationsFactory}. */
@ApplicationScoped
class FileOperationsFactoryImpl implements FileOperationsFactory {

  private final Clock clock;

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  FileOperationsFactoryImpl(Clock clock) {
    this.clock = clock;
  }

  @Override
  public FileOperations createFileOperations(FileIO fileIO) {
    return new FileOperationsImpl(fileIO);
  }
}

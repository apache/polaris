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
package org.apache.polaris.service.catalog.directory;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.service.catalog.spi.DirectoryScanService;

/** Lists the objects under the base location and replaces the content of the inventory table. */
@ApplicationScoped
@DefaultBean
public class DefaultDirectoryScanService implements DirectoryScanService {

  @Override
  public long scan(
      TableIdentifier identifier,
      Table table,
      FileIO sourceIO,
      String baseLocation,
      List<String> include,
      List<String> exclude) {
    return DirectoryScanner.scan(table, sourceIO, baseLocation, include, exclude);
  }
}

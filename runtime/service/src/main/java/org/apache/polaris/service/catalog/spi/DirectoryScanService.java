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
package org.apache.polaris.service.catalog.spi;

import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;

/**
 * Scans the objects of a directory and populates its inventory table.
 *
 * <p>Polaris provides a default implementation. To customize the scan (for example to compute
 * checksums, read object-store metadata or update the table incrementally), provide an {@code
 * ApplicationScoped} CDI bean implementing this interface: it replaces the default one.
 */
public interface DirectoryScanService {

  /**
   * @param identifier the directory identifier, also the identifier of its inventory table
   * @param table the directory inventory table to populate
   * @param sourceIO a {@link FileIO} with read and list access to {@code baseLocation}
   * @param baseLocation the location to scan
   * @param include regular expressions of the object URIs to include, {@code null} for all
   * @param exclude regular expressions of the object URIs to exclude, {@code null} for none
   * @return the number of objects recorded in the table
   */
  long scan(
      TableIdentifier identifier,
      Table table,
      FileIO sourceIO,
      String baseLocation,
      List<String> include,
      List<String> exclude);
}

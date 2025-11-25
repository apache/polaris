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
package org.apache.polaris.service.catalog;

/** An extension point for converting Iceberg REST API "prefix" values to Polaris Catalog names. */
public interface CatalogPrefixParser {

  /**
   * Produces the name of a Polaris catalog from the given Iceberg Catalog REST API "prefix".
   *
   * @param prefix the "prefix" according to the Iceberg REST Catalog API specification.
   * @return Polaris Catalog name
   */
  String prefixToCatalogName(String prefix);

  /**
   * Produces the "prefix" according to the Iceberg REST Catalog API specification for the given
   * Polaris catalog name.
   *
   * @param catalogName name of a Polaris catalog.
   * @return the "prefix" for the Iceberg REST client to be used for requests to the given catalog.
   */
  String catalogNameToPrefix(String catalogName);
}

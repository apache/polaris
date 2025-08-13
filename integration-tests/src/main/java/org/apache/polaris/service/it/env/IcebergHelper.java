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
package org.apache.polaris.service.it.env;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;

public final class IcebergHelper {
  private IcebergHelper() {}

  public static RESTCatalog restCatalog(
      PolarisApiEndpoints endpoints,
      String catalog,
      Map<String, String> extraProperties,
      String authToken) {
    RESTCatalog restCatalog = new RESTCatalog();

    ImmutableMap.Builder<String, String> propertiesBuilder =
        ImmutableMap.<String, String>builder()
            .put(
                org.apache.iceberg.CatalogProperties.URI, endpoints.catalogApiEndpoint().toString())
            .put(OAuth2Properties.TOKEN, authToken)
            .put(
                org.apache.iceberg.CatalogProperties.FILE_IO_IMPL,
                "org.apache.iceberg.inmemory.InMemoryFileIO")
            .put("warehouse", catalog)
            .put("header." + endpoints.realmHeaderName(), endpoints.realmId())
            .putAll(extraProperties);

    restCatalog.initialize("polaris", propertiesBuilder.buildKeepingLast());
    return restCatalog;
  }
}

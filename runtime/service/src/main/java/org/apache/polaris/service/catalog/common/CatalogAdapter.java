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
package org.apache.polaris.service.catalog.common;

import jakarta.ws.rs.core.SecurityContext;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.IdentifierParser;
import org.apache.polaris.service.catalog.IdentifierParserFactory;

public interface CatalogAdapter {
  CatalogPrefixParser getPrefixParser();

  IdentifierParserFactory getIdentifierParserFactory();

  default Namespace decodeNamespace(String namespace) {
    return RESTUtil.decodeNamespace(URLEncoder.encode(namespace, Charset.defaultCharset()));
  }

  default PolarisPrincipal validatePrincipal(SecurityContext securityContext) {
    var authenticatedPrincipal = securityContext.getUserPrincipal();
    if (authenticatedPrincipal instanceof PolarisPrincipal polarisPrincipal) {
      return polarisPrincipal;
    }
    throw new NotAuthorizedException("Failed to find authenticatedPrincipal in SecurityContext");
  }

  default IdentifierParser createParser(
      RealmContext realmContext, String prefix, SecurityContext securityContext) {
    PolarisPrincipal principal = validatePrincipal(securityContext);
    String catalogName = getPrefixParser().prefixToCatalogName(realmContext, prefix);
    return getIdentifierParserFactory().createParser(principal, catalogName);
  }

  default Namespace parseNamespace(
      RealmContext realmContext, String prefix, String namespace, SecurityContext securityContext) {
    return createParser(realmContext, prefix, securityContext).parseNamespace(namespace);
  }

  default TableIdentifier parseTableIdentifier(
      RealmContext realmContext,
      String prefix,
      String namespace,
      String table,
      SecurityContext securityContext) {
    return createParser(realmContext, prefix, securityContext)
        .parseTableIdentifier(namespace, table);
  }
}

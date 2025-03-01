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
package org.apache.polaris.service.context;

import java.util.Map;
import java.util.function.Function;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.polaris.core.context.RealmContext;

public interface RealmContextResolver {

  /**
   * Resolves the realm context for the given request.
   *
   * @return the resolved realm context
   * @throws UnresolvableRealmContextException if the realm context cannot be resolved
   */
  RealmContext resolveRealmContext(
      String requestURL, String method, String path, Function<String, String> headers);

  default RealmContext resolveRealmContext(
      String requestURL, String method, String path, Map<String, String> headers) {
    CaseInsensitiveMap caseInsensitiveMap = new CaseInsensitiveMap(headers);
    return resolveRealmContext(
        requestURL, method, path, (key) -> (String) caseInsensitiveMap.get(key));
  }
}

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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For local/dev testing, this resolver simply expects a custom bearer-token format that is a
 * semicolon-separated list of colon-separated key/value pairs that constitute the realm properties.
 *
 * <p>Example: principal:data-engineer;password:test;realm:acct123
 */
@ApplicationScoped
@Identifier("default")
public class DefaultCallContextResolver implements CallContextResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCallContextResolver.class);

  @Override
  public CallContext resolveCallContext(
      final RealmContext realmContext, String method, String path, Map<String, String> headers) {
    LOGGER
        .atDebug()
        .addKeyValue("realmContext", realmContext.getRealmIdentifier())
        .addKeyValue("method", method)
        .addKeyValue("path", path)
        .addKeyValue("headers", headers)
        .log("Resolving CallContext");
    return CallContext.of(realmContext);
  }
}

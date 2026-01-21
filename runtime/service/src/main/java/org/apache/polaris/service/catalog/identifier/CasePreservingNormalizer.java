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
package org.apache.polaris.service.catalog.identifier;

import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Identity normalizer that preserves the original casing of all identifiers.
 *
 * <p>This is the default behavior for case-sensitive catalogs.
 */
public class CasePreservingNormalizer implements IdentifierNormalizer {

  public static final CasePreservingNormalizer INSTANCE = new CasePreservingNormalizer();

  /** Use {@link #INSTANCE} instead. */
  private CasePreservingNormalizer() {}

  @Override
  public Namespace normalizeNamespace(Namespace namespace) {
    return namespace;
  }

  @Override
  public TableIdentifier normalizeTableIdentifier(TableIdentifier identifier) {
    return identifier;
  }

  @Override
  public String normalizeName(String name) {
    return name;
  }

  @Override
  public List<String> normalizeNames(List<String> names) {
    return names;
  }

  @Override
  public boolean isCaseNormalizing() {
    return false;
  }
}

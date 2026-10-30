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
package org.apache.polaris.service.catalog.semanticmodel;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.polaris.service.catalog.semanticmodel.types.SemanticModelDocument;

/**
 * Contract for validating an Apache Ossie semantic-model document at write time.
 *
 * <p>Implementations are expected to signal an invalid document by throwing an exception that maps
 * to HTTP 400 (e.g. {@link org.apache.iceberg.exceptions.BadRequestException}) with field-level
 * detail.
 */
public interface SemanticDocumentValidator {

  /**
   * Validates the given Ossie document, throwing on failure.
   *
   * <p>The caller has already rejected empty, oversized, and malformed {@code semantic_model}
   * bodies, so implementations receive the body pre-parsed and must not mutate it.
   *
   * @param document the request document, whose {@code version} selects the Ossie spec version
   * @param semanticModel the parsed {@code semantic_model} body
   */
  void validate(SemanticModelDocument document, JsonNode semanticModel);
}

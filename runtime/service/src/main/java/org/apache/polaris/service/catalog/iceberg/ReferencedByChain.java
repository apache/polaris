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
package org.apache.polaris.service.catalog.iceberg;

import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.rest.NamespaceUtils;

/**
 * Parser for the {@code referenced-by} query parameter of the Iceberg REST {@code loadTable},
 * {@code loadView} and {@code loadCredentials} endpoints.
 *
 * <p>The value is a comma-separated list of fully qualified view identifiers, outermost view first.
 * Each identifier is the namespace levels followed by the view name, all joined by the namespace
 * separator; a comma within a name is encoded as {@code %2C}.
 */
final class ReferencedByChain {

  private static final Pattern ENCODED_COMMA = Pattern.compile("%2C", Pattern.CASE_INSENSITIVE);

  private ReferencedByChain() {}

  /**
   * Parses and validates a {@code referenced-by} value.
   *
   * @param referencedBy the raw query parameter, possibly {@code null} or empty
   * @param target the table or view being loaded, which must not appear in the chain
   * @param maxDepth the maximum number of entries accepted
   * @return the chain, outermost first; empty if {@code referencedBy} is absent or empty
   * @throws BadRequestException if the value cannot be parsed, is too deep, repeats an entry or
   *     contains the target
   */
  static List<TableIdentifier> parse(String referencedBy, TableIdentifier target, int maxDepth) {
    List<TableIdentifier> chain = parse(referencedBy);
    if (chain.size() > maxDepth) {
      throw new BadRequestException(
          "Invalid referenced-by: chain has %d entries, the maximum is %d", chain.size(), maxDepth);
    }
    Set<TableIdentifier> seen = new HashSet<>();
    for (TableIdentifier entry : chain) {
      if (entry.equals(target)) {
        throw new BadRequestException(
            "Invalid referenced-by: chain contains the target %s", target);
      }
      if (!seen.add(entry)) {
        throw new BadRequestException("Invalid referenced-by: chain repeats %s", entry);
      }
    }
    return chain;
  }

  /**
   * Parses a {@code referenced-by} value without validating the chain as a whole.
   *
   * @throws BadRequestException if an entry cannot be parsed
   */
  static List<TableIdentifier> parse(String referencedBy) {
    if (referencedBy == null || referencedBy.isEmpty()) {
      return List.of();
    }
    List<TableIdentifier> chain = new ArrayList<>();
    for (String entry : Splitter.on(',').split(referencedBy)) {
      chain.add(parseEntry(entry));
    }
    return List.copyOf(chain);
  }

  private static TableIdentifier parseEntry(String entry) {
    List<String> parts = Splitter.on(NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR).splitToList(entry);
    // At least one namespace level and a name, none of them empty
    if (parts.size() < 2 || parts.stream().anyMatch(String::isEmpty)) {
      throw new BadRequestException("Invalid referenced-by: malformed entry '%s'", entry);
    }
    String[] decoded =
        parts.stream()
            .map(part -> ENCODED_COMMA.matcher(part).replaceAll(","))
            .toArray(String[]::new);
    int last = decoded.length - 1;
    return TableIdentifier.of(Namespace.of(Arrays.copyOfRange(decoded, 0, last)), decoded[last]);
  }
}

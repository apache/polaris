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
package org.apache.polaris.service.types;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public class PolicyIdentifier {

  private static final Splitter DOT = Splitter.on('.');

  private final Namespace namespace;
  private final String name;

  public static PolicyIdentifier of(String... names) {
    Preconditions.checkArgument(names != null, "Cannot create policy identifier from null array");
    Preconditions.checkArgument(names.length > 0, "Cannot create policy identifier without a name");
    return new PolicyIdentifier(
        Namespace.of(Arrays.copyOf(names, names.length - 1)), names[names.length - 1]);
  }

  public static PolicyIdentifier of(Namespace namespace, String name) {
    return new PolicyIdentifier(namespace, name);
  }

  public static PolicyIdentifier parse(String identifier) {
    Preconditions.checkArgument(identifier != null, "Cannot parse policy identifier: null");
    Iterable<String> parts = DOT.split(identifier);
    return PolicyIdentifier.of(Iterables.toArray(parts, String.class));
  }

  public static PolicyIdentifier fromTableIdentifier(TableIdentifier tableIdentifier) {
    return new PolicyIdentifier(tableIdentifier.namespace(), tableIdentifier.name());
  }

  private PolicyIdentifier(Namespace namespace, String name) {
    Preconditions.checkArgument(name != null && !name.isEmpty(), "Invalid name: null or empty");
    Preconditions.checkArgument(namespace != null, "Invalid Namespace: null");
    this.namespace = namespace;
    this.name = name;
  }

  /**
   * Whether the namespace is empty.
   *
   * @return true if the namespace is not empty, false otherwise
   */
  public boolean hasNamespace() {
    return !namespace.isEmpty();
  }

  /** Returns the identifier namespace. */
  public Namespace namespace() {
    return namespace;
  }

  /** Returns the identifier name. */
  public String name() {
    return name;
  }

  public TableIdentifier toTableIdentifier() {
    return TableIdentifier.of(namespace, name);
  }

  public PolicyIdentifier toLowerCase() {
    String[] newLevels =
        Arrays.stream(namespace().levels()).map(String::toLowerCase).toArray(String[]::new);
    String newName = name().toLowerCase(Locale.ROOT);
    return PolicyIdentifier.of(Namespace.of(newLevels), newName);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    PolicyIdentifier that = (PolicyIdentifier) other;
    return namespace.equals(that.namespace) && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, name);
  }

  @Override
  public String toString() {
    if (hasNamespace()) {
      return namespace.toString() + "." + name;
    } else {
      return name;
    }
  }
}

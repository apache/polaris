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
package org.apache.polaris.persistence.nosql.metastore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.immutables.value.Value;

@Value.Style(underrideToString = "asDotDelimitedString")
@PolarisImmutable
public interface ContentIdentifier {
  @Value.Parameter
  @JsonValue
  List<String> elements();

  static ContentIdentifier identifier(List<String> elements) {
    return ImmutableContentIdentifier.of(elements);
  }

  static ContentIdentifier identifier(String[] namespace, String name) {
    return ImmutableContentIdentifier.builder().addElements(namespace).addElements(name).build();
  }

  static ContentIdentifier identifier(String... elements) {
    return ImmutableContentIdentifier.of(List.of(elements));
  }

  static ContentIdentifier identifierFor(PolicyIdentifier identifier) {
    return identifier(identifier.namespace().levels(), identifier.name());
  }

  static ContentIdentifier identifierFor(TableIdentifier identifier) {
    return identifier(identifier.namespace().levels(), identifier.name());
  }

  static ContentIdentifier identifierFor(Namespace namespace) {
    return identifier(namespace.levels());
  }

  /**
   * Constructs a {@link ContentIdentifier} from a storage location without the scheme.
   *
   * <p>This is used for the locations index for location-overlap checks.
   *
   * <p>Valid inputs follow the values returned by {@link StorageLocation#withoutScheme()
   * StorageLocation.of(previousBaseLocation).withoutScheme()}, which can, in case of "file"
   * locations, contain system-specific file separators.
   */
  static ContentIdentifier identifierFromLocationString(String locationString) {
    var builder = builder();
    var len = locationString.length();
    var off = -1;
    for (var i = 0; i < len; i++) {
      var c = locationString.charAt(i);
      checkArgument(c >= ' ' && c != 127, "Control characters are forbidden in locations");
      if (c == '/' || c == '\\') {
        if (off != -1) {
          builder.addElements(locationString.substring(off, i));
          off = -1;
        }
      } else {
        if (off == -1) {
          off = i;
        }
      }
    }
    if (off != -1) {
      builder.addElements(locationString.substring(off));
    }
    return builder.build();
  }

  default ContentIdentifier parent() {
    var elems = elements();
    checkState(!elems.isEmpty(), "Empty namespace has no parent");
    return ImmutableContentIdentifier.of(elems.subList(0, elems.size() - 1));
  }

  default boolean isEmpty() {
    return elements().isEmpty();
  }

  default int length() {
    return elements().size();
  }

  default String leafName() {
    var elems = elements();
    return elems.isEmpty() ? "" : elems.getLast();
  }

  default ContentIdentifier childOf(String childName) {
    var elems = elements();
    var newElements = new ArrayList<String>(elems.size() + 1);
    newElements.addAll(elems);
    newElements.add(childName);
    return ImmutableContentIdentifier.of(newElements);
  }

  default String asDotDelimitedString() {
    return String.join(".", elements());
  }

  static ImmutableContentIdentifier.Builder builder() {
    return ImmutableContentIdentifier.builder();
  }

  default IndexKey toIndexKey() {
    return IndexKey.key(String.join("\u0000", elements()));
  }

  default boolean startsWith(ContentIdentifier other) {
    var elems = elements();
    var otherElems = other.elements();
    var otherSize = otherElems.size();
    if (otherSize > elems.size()) {
      return false;
    }
    for (int i = 0; i < otherSize; i++) {
      if (!elems.get(i).equals(otherElems.get(i))) {
        return false;
      }
    }
    return true;
  }

  @CanIgnoreReturnValue
  static ImmutableContentIdentifier.Builder indexKeyToIdentifierBuilder(
      IndexKey indexKey, ImmutableContentIdentifier.Builder builder) {
    var str = indexKey.toString();
    var l = str.length();
    for (var i = 0; i < l; ) {
      var iNull = str.indexOf(0, i);
      if (iNull == -1) {
        builder.addElements(str.substring(i));
        return builder;
      }
      builder.addElements(str.substring(i, iNull));
      i = iNull + 1;
    }
    return builder;
  }

  static ImmutableContentIdentifier.Builder indexKeyToIdentifierBuilder(IndexKey indexKey) {
    return indexKeyToIdentifierBuilder(indexKey, ImmutableContentIdentifier.builder());
  }

  static ContentIdentifier indexKeyToIdentifier(IndexKey indexKey) {
    return indexKeyToIdentifierBuilder(indexKey).build();
  }
}

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

import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.immutables.value.Value;

@Value.Style(underrideToString = "asDotDelimitedString")
@PolarisImmutable
public interface Identifier {
  @Value.Parameter
  @JsonValue
  List<String> elements();

  static Identifier identifier(List<String> elements) {
    return ImmutableIdentifier.of(elements);
  }

  static Identifier identifier(String... elements) {
    return ImmutableIdentifier.of(List.of(elements));
  }

  default Identifier parent() {
    var elems = elements();
    checkState(!elems.isEmpty(), "Empty namespace has no parent");
    return ImmutableIdentifier.of(elems.subList(0, elems.size() - 1));
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

  default Identifier childOf(String childName) {
    var elems = elements();
    var newElements = new ArrayList<String>(elems.size() + 1);
    newElements.addAll(elems);
    newElements.add(childName);
    return ImmutableIdentifier.of(newElements);
  }

  default String asDotDelimitedString() {
    return String.join(".", elements());
  }

  static ImmutableIdentifier.Builder builder() {
    return ImmutableIdentifier.builder();
  }

  static IndexKey identifierToIndexKey(Identifier identifier) {
    return IndexKey.key(String.join("\u0000", identifier.elements()));
  }

  @CanIgnoreReturnValue
  static ImmutableIdentifier.Builder indexKeyToIdentifierBuilder(
      IndexKey indexKey, ImmutableIdentifier.Builder builder) {
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

  static ImmutableIdentifier.Builder indexKeyToIdentifierBuilder(IndexKey indexKey) {
    return indexKeyToIdentifierBuilder(indexKey, ImmutableIdentifier.builder());
  }

  static Identifier indexKeyToIdentifier(IndexKey indexKey) {
    return indexKeyToIdentifierBuilder(indexKey).build();
  }
}

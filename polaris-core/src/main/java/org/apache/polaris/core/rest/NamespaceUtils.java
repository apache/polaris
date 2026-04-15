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

package org.apache.polaris.core.rest;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.iceberg.catalog.Namespace;

/** Utility methods for encoding and decoding {@link Namespace} values. */
public final class NamespaceUtils {

  /**
   * The default namespace separator: the ASCII Unit Separator character (U+001F).
   *
   * <p>This is the same separator declared in Iceberg's {@link org.apache.iceberg.rest.RESTUtil}
   * class, but it's private there and cannot be referenced.
   */
  public static final String DEFAULT_NAMESPACE_SEPARATOR = "\u001f";

  private NamespaceUtils() {}

  /** Joins the levels of a namespace into a single string using the given {@code separator}. */
  public static String joinNamespace(Namespace namespace, String separator) {
    return Joiner.on(separator).join(namespace.levels());
  }

  /**
   * Splits the string using the given {@code separator} and returns the corresponding namespace.
   */
  public static Namespace splitNamespace(String namespace, String separator) {
    return Namespace.of(Iterables.toArray(Splitter.on(separator).split(namespace), String.class));
  }
}

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

package org.apache.polaris.service.storage;

import java.net.URI;
import org.apache.polaris.core.storage.StorageLocation;

/**
 * A component that translates HTTP(S) URLs into a logical, object-store-specific {@link
 * StorageLocation}.
 */
public interface StorageLocationTranslator {

  interface Context {
    Context EMPTY = new Context() {};
  }

  /**
   * Translates the given HTTP(S) URL into a {@link StorageLocation}.
   *
   * @param uri the HTTP(S) URL to translate
   * @param context the translation {@link Context}
   * @return the translated {@link StorageLocation}
   * @throws IllegalArgumentException if the URI is not a valid HTTP(S) URL or cannot be translated
   */
  StorageLocation translate(URI uri, Context context);
}

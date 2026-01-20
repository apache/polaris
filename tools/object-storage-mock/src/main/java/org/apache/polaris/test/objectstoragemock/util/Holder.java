/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// CODE_COPIED_TO_POLARIS from Project Nessie 0.106.1
package org.apache.polaris.test.objectstoragemock.util;

import org.apache.polaris.test.objectstoragemock.S3Resource;

/**
 * Helper objects used to hold an object instance, see {@link
 * S3Resource#listObjectsInsideBucket(String, String, String, String, String, int, int, String,
 * String, String)}.
 */
public final class Holder<T> {
  private T value;

  public void set(T value) {
    this.value = value;
  }

  public T get() {
    return value;
  }
}

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
package org.apache.polaris.persistence.nosql.impl;

import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

final class MultiByteArrayInputStream extends InputStream {
  private final Iterator<byte[]> sources;
  private byte[] current;
  private int pos;

  public MultiByteArrayInputStream(List<byte[]> sources) {
    this.sources = sources.iterator();
  }

  @Override
  public int read(@Nonnull byte[] b, int off, int len) {
    while (true) {
      if (checkCurrentEof()) {
        return -1;
      }

      if (pos >= current.length) {
        current = null;
        continue;
      }

      var remain = current.length - pos;
      var amount = Math.min(len, remain);
      System.arraycopy(current, pos, b, off, amount);
      pos += amount;
      return amount;
    }
  }

  @Override
  public int read() {
    while (true) {
      if (checkCurrentEof()) {
        return -1;
      }

      if (pos >= current.length) {
        current = null;
        continue;
      }
      return current[pos++] & 0xFF;
    }
  }

  private boolean checkCurrentEof() {
    if (current == null) {
      if (!sources.hasNext()) {
        return true;
      }
      current = requireNonNull(sources.next(), "No source byte[] element is null");
      pos = 0;
    }
    return false;
  }
}

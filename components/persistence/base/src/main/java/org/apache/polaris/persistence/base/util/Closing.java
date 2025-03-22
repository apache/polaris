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
package org.apache.polaris.persistence.base.util;

import static java.util.Arrays.asList;

import java.util.List;

public final class Closing {
  private Closing() {}

  public static void closeMultiple(AutoCloseable... closeables) throws Exception {
    closeMultiple(asList(closeables));
  }

  public static void closeMultiple(List<AutoCloseable> closeables) throws Exception {
    Exception ex = null;
    for (AutoCloseable closeable : closeables) {
      try {
        if (closeable != null) {
          closeable.close();
        }
      } catch (Exception e) {
        if (ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }
}

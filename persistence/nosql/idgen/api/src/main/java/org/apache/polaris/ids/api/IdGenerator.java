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
package org.apache.polaris.ids.api;

public interface IdGenerator {
  /** Generate a new, unique ID. */
  long generateId();

  /** Generate the system ID for a node, solely used by/for node management purposes. */
  long systemIdForNode(int nodeId);

  default String describeId(long id) {
    return Long.toString(id);
  }

  IdGenerator NONE =
      new IdGenerator() {
        @Override
        public long generateId() {
          throw new UnsupportedOperationException("NONE IdGenerator");
        }

        @Override
        public long systemIdForNode(int nodeId) {
          throw new UnsupportedOperationException("NONE IdGenerator");
        }
      };
}

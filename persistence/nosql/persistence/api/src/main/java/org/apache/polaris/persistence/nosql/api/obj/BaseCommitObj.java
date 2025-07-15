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
package org.apache.polaris.persistence.nosql.api.obj;

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Optional;

public interface BaseCommitObj extends Obj {
  /**
   * Monotonically increasing counter representing the number of commits since the "beginning of
   * time".
   */
  long seq();

  /**
   * Zero, one or more parent-entry hashes of this commit, the nearest parent first.
   *
   * <p>This is an internal attribute used to more efficiently page through the commit log.
   *
   * <p>Only the first, the nearest parent shall be exposed to clients.
   *
   * <p>This is a {@code long[]} for more efficient serialization wrt space.
   */
  long[] tail();

  default Optional<ObjRef> directParent() {
    var t = tail();
    return t.length == 0 ? Optional.empty() : Optional.of(objRef(type(), t[0]));
  }

  interface Builder<O extends BaseCommitObj, B extends Builder<O, B>> {
    @CanIgnoreReturnValue
    B id(long id);

    @CanIgnoreReturnValue
    B seq(long seq);

    @CanIgnoreReturnValue
    B tail(long[] tail);

    O build();
  }
}

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
package org.apache.polaris.persistence.coretypes.acl;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.polaris.persistence.api.index.IndexContainer;
import org.apache.polaris.persistence.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.api.obj.ObjRef;

public interface GrantsObj extends BaseCommitObj {

  /** Index of securable keys to {@link AclObj}s. */
  IndexContainer<ObjRef> acls();

  interface Builder<O extends GrantsObj, B extends BaseCommitObj.Builder<O, B>>
      extends BaseCommitObj.Builder<O, B> {
    @CanIgnoreReturnValue
    B from(GrantsObj container);

    @CanIgnoreReturnValue
    B acls(IndexContainer<ObjRef> acls);
  }
}

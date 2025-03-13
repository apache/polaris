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
package org.apache.polaris.persistence.coretypes.content;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Optional;

/** Base for all catalog content objects, including namespaces, tables, views, etc. */
public interface TableLikeObj extends PolicyAttachableContentObj {

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> metadataLocation();

  interface Builder<O extends TableLikeObj, B extends Builder<O, B>>
      extends PolicyAttachableContentObj.Builder<O, B> {

    @CanIgnoreReturnValue
    B from(TableLikeObj from);

    @CanIgnoreReturnValue
    B metadataLocation(String metadataLocation);
  }
}

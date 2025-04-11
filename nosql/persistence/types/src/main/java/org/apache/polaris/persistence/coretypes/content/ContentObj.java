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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Instant;
import java.util.Optional;
import org.apache.polaris.persistence.coretypes.catalog.CatalogObjBase;

/** Base for all catalog content objects, including namespaces, tables, views, etc. */
public interface ContentObj extends CatalogObjBase {

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @JsonFormat(shape = JsonFormat.Shape.NUMBER)
  Optional<Instant> lastNotificationTimestamp();

  interface Builder<O extends ContentObj, B extends Builder<O, B>>
      extends CatalogObjBase.Builder<O, B> {

    @CanIgnoreReturnValue
    B from(ContentObj from);
  }
}

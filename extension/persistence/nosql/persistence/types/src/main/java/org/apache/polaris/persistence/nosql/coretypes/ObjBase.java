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
package org.apache.polaris.persistence.nosql.coretypes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Instant;
import java.util.Map;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.immutables.value.Value;

public interface ObjBase extends Obj {

  String name();

  /**
   * The <em>stable</em> ID for this object, remains the same for each update of the logical object.
   * Raw persistence model {@link #id()} are unique for each persisted "version", because persisted
   * objects are immutable. This value is assigned once, but never changed.
   */
  long stableId();

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  @Value.Default
  default long parentStableId() {
    return 0L;
  }

  @Value.Default
  default int entityVersion() {
    return 1;
  }

  Instant createTimestamp();

  Instant updateTimestamp();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> properties();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> internalProperties();

  interface Builder<O extends ObjBase, B extends Builder<O, B>> extends Obj.Builder<O, B> {

    @CanIgnoreReturnValue
    B from(ObjBase from);

    @CanIgnoreReturnValue
    B stableId(long stableId);

    @CanIgnoreReturnValue
    B entityVersion(int entityVersion);

    @CanIgnoreReturnValue
    B name(String name);

    @CanIgnoreReturnValue
    B parentStableId(long parentStableId);

    @CanIgnoreReturnValue
    B createTimestamp(Instant createTimestamp);

    @CanIgnoreReturnValue
    B updateTimestamp(Instant updateTimestamp);

    @CanIgnoreReturnValue
    B putProperty(String key, String value);

    @CanIgnoreReturnValue
    B putProperty(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    B properties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    B putAllProperties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    B putInternalProperty(String key, String value);

    @CanIgnoreReturnValue
    B putInternalProperty(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    B internalProperties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    B putAllInternalProperties(Map<String, ? extends String> entries);
  }
}

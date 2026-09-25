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
package org.apache.polaris.core.tag;

import java.util.OptionalLong;

/**
 * One assignment row a caller judged inert, together with the state of the target it judged it
 * from. The judgement is only as good as that state: a row is inert because of something about its
 * target, so a delete acting on the judgement has to establish that the target has not changed
 * since.
 *
 * <p>The term is the target entity's {@code entity_version}, which every target carries whatever
 * its type, and which moves on every write to the entity: a table commit persists a new metadata
 * location through the shared change path, and that increments the version. So a column row judged
 * inert because the table's current schema no longer has its field id stops matching the moment
 * that table commits again, including the commit that would make the field current once more.
 *
 * <p>{@link OptionalLong#empty()} means the target did not resolve when the caller judged the row:
 * the orphan case. It must still not resolve at delete time. Entity ids are never reused, so a
 * target that resolves again where absence was recorded is not the same target, and the delete
 * refuses rather than guesses.
 *
 * <p>This is a separate type rather than more fields on {@link TagAssignmentIdentity} because the
 * identity's equality is what the delete's membership check compares, and it must stay the identity
 * of the row, not of the row plus a moment in time.
 */
public record ClassifiedAssignment(
    TagAssignmentIdentity identity, OptionalLong targetEntityVersion) {

  public static ClassifiedAssignment of(
      TagAssignmentRecord record, OptionalLong targetEntityVersion) {
    return new ClassifiedAssignment(TagAssignmentIdentity.of(record), targetEntityVersion);
  }
}

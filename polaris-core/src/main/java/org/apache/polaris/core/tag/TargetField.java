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

/**
 * One (target, field) key identifying the tag assignment rows for a single level of a tag read:
 * every row whose (targetCatalogId, targetId, fieldId) exactly matches.
 *
 * <p>Used by the bulk assignment read that loads every level of a tag traversal in one call,
 * instead of one persistence call per level.
 *
 * @param targetCatalogId canonical containing-catalog id of the target, see {@link
 *     TagAssignmentRecord#containingCatalogId}
 * @param targetId id of the target entity; for a column assignment, the containing table
 * @param fieldId 0 for a whole-object assignment, else the top-level Iceberg field id
 */
public record TargetField(long targetCatalogId, long targetId, int fieldId) {}

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
package org.apache.polaris.persistence.nosql.metastore;

import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.SUCCESS;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;

final class MutationResults {
  private final List<BaseResult> results;
  // TODO populate and process 'aclsToRemove'
  private final List<GrantTriplet> aclsToRemove;
  private final List<PolarisBaseEntity> droppedEntities;
  private final List<IndexKey> policyIndexKeysToRemove = new ArrayList<>();

  boolean anyChange;
  boolean hardFailure;

  private MutationResults(
      List<BaseResult> results,
      List<GrantTriplet> aclsToRemove,
      List<PolarisBaseEntity> droppedEntities) {
    this.results = results;
    this.aclsToRemove = aclsToRemove;
    this.droppedEntities = droppedEntities;
  }

  MutationResults(BaseResult single) {
    this(List.of(single), List.of(), List.of());
  }

  static MutationResults singleEntityResult(BaseResult.ReturnStatus returnStatus) {
    return new MutationResults(new EntityResult(returnStatus, null));
  }

  static MutationResults singleEntityResult(PolarisBaseEntity entity) {
    return new MutationResults(new EntityResult(entity));
  }

  static MutationResults newMutableMutationResults() {
    return new MutationResults(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
  }

  List<BaseResult> results() {
    return results;
  }

  List<GrantTriplet> aclsToRemove() {
    return aclsToRemove;
  }

  List<PolarisBaseEntity> droppedEntities() {
    return droppedEntities;
  }

  List<IndexKey> policyIndexKeysToRemove() {
    return policyIndexKeysToRemove;
  }

  void addPolicyIndexKeyToRemove(IndexKey indexKey) {
    policyIndexKeysToRemove.add(indexKey);
  }

  void entityResult(PolarisBaseEntity entity) {
    add(new EntityResult(entity));
    anyChange = true;
  }

  void entityResultNoChange(PolarisBaseEntity entity) {
    add(new EntityResult(entity));
  }

  void unchangedEntityResult(PolarisBaseEntity entity) {
    add(new EntityResult(entity));
  }

  void entityResult(BaseResult.ReturnStatus returnStatus) {
    entityResult(returnStatus, null);
  }

  void entityResult(BaseResult.ReturnStatus returnStatus, String extraInformation) {
    add(new EntityResult(returnStatus, extraInformation));
    hardFailure |= returnStatus != SUCCESS;
  }

  void dropResult(PolarisBaseEntity entity) {
    add(new DropEntityResult());
    droppedEntities.add(entity);
    anyChange = true;
  }

  void dropResult(BaseResult.ReturnStatus returnStatus) {
    dropResult(returnStatus, null);
  }

  void dropResult(BaseResult.ReturnStatus returnStatus, String extraInformation) {
    add(new DropEntityResult(returnStatus, extraInformation));
    hardFailure |= returnStatus != SUCCESS;
  }

  private void add(BaseResult result) {
    results.add(result);
    hardFailure |= result.getReturnStatus() != SUCCESS;
  }

  String failuresAsString() {
    if (!hardFailure) {
      return "(none)";
    }

    return results.stream()
        .filter(result -> result.getReturnStatus() != SUCCESS)
        .map(result -> result.getReturnStatus().name())
        .collect(Collectors.joining(", "));
  }

  public Optional<BaseResult> firstFailure() {
    if (!hardFailure) {
      return Optional.empty();
    }
    return results.stream().filter(result -> result.getReturnStatus() != SUCCESS).findFirst();
  }
}

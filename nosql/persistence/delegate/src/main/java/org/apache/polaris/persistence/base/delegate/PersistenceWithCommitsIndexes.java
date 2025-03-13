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
package org.apache.polaris.persistence.base.delegate;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.persistence.api.Persistence;
import org.apache.polaris.persistence.api.commit.Commits;
import org.apache.polaris.persistence.api.commit.Committer;
import org.apache.polaris.persistence.api.index.Index;
import org.apache.polaris.persistence.api.index.IndexContainer;
import org.apache.polaris.persistence.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.api.index.UpdatableIndex;
import org.apache.polaris.persistence.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.commits.CommitFactory;
import org.apache.polaris.persistence.indexes.IndexesProvider;

/**
 * Lets the commit and index related functions delegate to the {@link Persistence} on their
 * "current" delegation level. The function implementations require access to the implementations,
 * which should not leak into the {@code polaris-persistence-api} module. In other words: {@link
 * Persistence} implementations and delegates need this interface, users of {@link Persistence} must
 * not use this interface.
 *
 * <p>This means, that the {@link Persistence} implementation in {@code polaris-persistence-cache}
 * must create {@link Commits}/{@link Committer}/{@link Index}/{@link UpdatableIndex}
 * implementations that use {@code this} using the cache. Similar for "observing" and
 * "retained-collector".
 *
 * <p>This interface is used by {@code polaris-persistence-impl}, {@code polaris-persistence-cache},
 * {@code polaris-persistence-maintenance-impl} and {@code polaris-persistence-cdi}.
 */
public interface PersistenceWithCommitsIndexes extends Persistence {

  @Override
  default Commits commits() {
    return CommitFactory.newCommits(this);
  }

  @Override
  default <REF_OBJ extends BaseCommitObj, RESULT> Committer<REF_OBJ, RESULT> createCommitter(
      @Nonnull String refName,
      @Nonnull Class<REF_OBJ> referencedObjType,
      @Nonnull Class<RESULT> resultType) {
    return CommitFactory.newCommitter(this, refName, referencedObjType, resultType);
  }

  @Override
  default <V> Index<V> buildReadIndex(
      @Nullable IndexContainer<V> indexContainer,
      @Nonnull IndexValueSerializer<V> indexValueSerializer) {
    return IndexesProvider.buildReadIndex(indexContainer, this, indexValueSerializer);
  }

  @Override
  default <V> UpdatableIndex<V> buildWriteIndex(
      @Nullable IndexContainer<V> indexContainer,
      @Nonnull IndexValueSerializer<V> indexValueSerializer) {
    return IndexesProvider.buildWriteIndex(indexContainer, this, indexValueSerializer);
  }
}

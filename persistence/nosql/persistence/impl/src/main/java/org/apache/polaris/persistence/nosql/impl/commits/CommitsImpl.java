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
package org.apache.polaris.persistence.nosql.impl.commits;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import com.google.common.collect.AbstractIterator;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import org.agrona.collections.LongArrayList;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.commit.Commits;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

final class CommitsImpl implements Commits {
  private final Persistence persistence;
  private static final int REVERSE_COMMIT_FETCH_SIZE = 20;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  CommitsImpl(Persistence persistence) {
    this.persistence = persistence;
  }

  @Override
  public <C extends BaseCommitObj> Iterator<C> commitLogReversed(
      String refName, long offset, Class<C> clazz) {
    var headOpt = persistence.fetchReferenceHead(refName, clazz);
    if (headOpt.isEmpty()) {
      return emptyIterator();
    }

    var head = headOpt.get();
    var type = head.type().id();

    // find commit with Obj.id() == offset, memoize visited commits

    // Contains the seen IDs, without the 'offset', in _natural_ order (most recent commit ID first)
    var visited = new LongArrayList();

    // TODO add safeguard to limit the work done when finding the commit with ID 'offset'

    // Only walk, if the most recent commit ID is != offset
    if (head.id() == offset) {
      return emptyIterator();
    }

    visited.add(head.id());
    var tail = head.tail();
    outer:
    while (tail.length != 0) {
      for (var tailId : tail) {
        if (tailId == offset) {
          break outer;
        }
        visited.add(tailId);
      }

      while (!visited.isEmpty()) {
        var idx = visited.size() - 1;
        var cid = objRef(type, visited.getLong(idx), 1);
        var commit = persistence.fetch(cid, clazz);
        if (commit != null) {
          tail = commit.tail();
          break;
        }

        // If the commit with the last ID in 'visited' was not found, ignore it and try the next
        // recent commit. This is a legit case when the commit log gets truncated.
        visited.removeAt(idx);
      }
    }

    // return iterator

    return new AbstractIterator<>() {
      private int index = visited.size();

      private Iterator<C> pageIter = emptyIterator();

      @Override
      protected C computeNext() {
        while (true) {
          if (pageIter.hasNext()) {
            var r = pageIter.next();
            if (r == null) {
              // stop at commits that do not exist, the history has been cut at that point.
              return endOfData();
            }
            return r;
          }

          if (index == 0) {
            return endOfData();
          }

          var ids = new ArrayList<ObjRef>(REVERSE_COMMIT_FETCH_SIZE);
          for (var i = 0; i < REVERSE_COMMIT_FETCH_SIZE; i++) {
            ids.add(objRef(type, visited.getLong(--index), 1));
            if (index == 0) {
              break;
            }
          }

          if (ids.isEmpty()) {
            return endOfData();
          }

          var commits = persistence.fetchMany(clazz, ids.toArray(new ObjRef[0]));
          pageIter = Arrays.asList(commits).iterator();
        }
      }
    };
  }

  @Override
  public <C extends BaseCommitObj> Iterator<C> commitLog(
      String refName, OptionalLong offset, Class<C> clazz) {
    var headOpt = persistence.fetchReferenceHead(refName, clazz);
    if (headOpt.isEmpty()) {
      return emptyIterator();
    }

    var head = headOpt.get();
    var type = head.type().id();

    // TODO add safeguard to limit the work done when finding the commit with ID 'offset'

    if (offset.isPresent()) {
      var off = offset.getAsLong();

      // Only walk, if the most recent commit ID is != offset
      if (head.id() == off) {
        return singletonList(head).iterator();
      }

      var tail = head.tail();
      outer:
      while (tail.length != 0) {
        var lastId = 0L;
        for (var tailId : tail) {
          if (tailId == off) {
            head = null; // force fetch
            break outer;
          }
          lastId = tailId;
        }

        var id = objRef(type, lastId, 1);
        head = persistence.fetch(id, clazz);

        if (head == null || head.id() == off) {
          break;
        }

        tail = head.tail();
      }

      if (head == null) {
        var id = objRef(type, off, 1);
        head = persistence.fetch(id, clazz);
      }
    }

    if (head == null) {
      return emptyIterator();
    }

    var headIter = List.of(head).iterator();
    return new AbstractIterator<>() {
      private C lastCommit;

      private Iterator<C> pageIter = headIter;

      @Override
      protected C computeNext() {
        while (true) {
          if (pageIter.hasNext()) {
            var c = pageIter.next();
            if (c == null) {
              // stop at commits that do not exist, the history has been cut at that point.
              return endOfData();
            }
            lastCommit = c;
            return c;
          }

          var tail = lastCommit.tail();
          if (tail.length == 0) {
            return endOfData();
          }
          lastCommit = null;

          var ids = new ObjRef[REVERSE_COMMIT_FETCH_SIZE];
          for (var i = 0; i < REVERSE_COMMIT_FETCH_SIZE && i < tail.length; i++) {
            ids[i] = objRef(type, tail[i], 1);
          }

          var page = persistence.fetchMany(clazz, ids);
          pageIter = Arrays.asList(page).iterator();
        }
      }
    };
  }
}

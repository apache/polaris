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
package org.apache.polaris.persistence.nosql.authz.api;

import static java.util.Collections.emptyIterator;

import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;

final class Constants {

  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private Constants() {}

  static final PrivilegeSet EMPTY_PRIVILEGE_SET =
      new PrivilegeSet() {
        @Override
        public boolean contains(Privilege privilege) {
          return false;
        }

        @Override
        public Iterator<Privilege> iterator(Privileges privileges) {
          return emptyIterator();
        }

        @Override
        public boolean isEmpty() {
          return true;
        }

        @Override
        public byte[] toByteArray() {
          return EMPTY_BYTE_ARRAY;
        }

        @Override
        public boolean contains(Object o) {
          return false;
        }

        @Override
        public boolean containsAll(@Nonnull Collection<?> c) {
          return false;
        }

        @Override
        public boolean containsAny(Iterable<? extends Privilege> privilege) {
          return false;
        }

        @Override
        public int size() {
          return 0;
        }

        @Override
        @Nonnull
        public Iterator<Privilege> iterator() {
          return emptyIterator();
        }

        @Override
        @Nonnull
        public Object[] toArray() {
          return new Object[0];
        }

        @Override
        @Nonnull
        public <T> T[] toArray(T[] a) {
          @SuppressWarnings("unchecked")
          var r = (T[]) new Object[a.length];
          return r;
        }

        @Override
        public boolean add(Privilege privilege) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(@Nonnull Collection<? extends Privilege> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(@Nonnull Collection<?> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(@Nonnull Collection<?> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object obj) {
          if (obj instanceof PrivilegeSet privilegeSet) {
            return privilegeSet.isEmpty();
          }
          return false;
        }

        @Override
        public int hashCode() {
          return -1;
        }

        @Override
        public String toString() {
          return "PrivilegeSet{}";
        }
      };
}

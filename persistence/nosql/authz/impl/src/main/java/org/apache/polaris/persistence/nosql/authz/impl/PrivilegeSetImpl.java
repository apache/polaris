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
package org.apache.polaris.persistence.nosql.authz.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import org.apache.polaris.persistence.nosql.authz.api.Privilege;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeSet;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;

/**
 * Represents a set of {@link Privilege}s, implemented with a bit-map.
 *
 * <p>Also provides JSON serializer that is capable of serializing using the privileges in a
 * space-efficient binary format (the bit-map, if the current JSON view is {@code StorageView}), and
 * a verbose textual representation (for "external" serialization).
 */
record PrivilegeSetImpl(Privileges privileges, byte[] bytes) implements PrivilegeSet {
  private PrivilegeSetImpl(Privileges privileges, PrivilegeSetBuilderImpl builder) {
    this(privileges, builder.bitSet.toByteArray());
  }

  @Override
  public int size() {
    var size = 0;
    for (var b : bytes) {
      var i = b & 0xFF;
      size += Integer.bitCount(i);
    }
    return size;
  }

  @Override
  public boolean isEmpty() {
    return bytes.length == 0;
  }

  @Override
  public byte[] toByteArray() {
    return Arrays.copyOf(bytes, bytes.length);
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public Object[] toArray() {
    return toArray(new Privilege[0]);
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public <T> T[] toArray(T[] a) {
    var size = size();
    var arrType = requireNonNull(a).getClass().getComponentType();
    checkArgument(arrType.isAssignableFrom(Privilege.class));
    var arr = (Object[]) a;
    if (arr.length < size) {
      arr = Arrays.copyOf(a, size);
    }

    var i = 0;
    for (Privilege privilege : this) {
      arr[i++] = privilege;
    }

    @SuppressWarnings("unchecked")
    var r = (T[]) arr;
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

  @SuppressWarnings("NullableProblems")
  @Override
  public boolean addAll(Collection<? extends Privilege> c) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean contains(Object privilege) {
    return privilege instanceof Privilege p && contains(p);
  }

  @Override
  public boolean contains(Privilege privilege) {
    return privilege.mustMatchAll()
        ? containsMustMatchAll(privilege)
        : containsMustMatchAny(privilege);
  }

  private boolean containsMustMatchAll(Privilege privilege) {
    var arr = this.bytes;
    for (Privilege.IndividualPrivilege p : privilege.resolved()) {
      var id = privileges.idForName(p.name());
      var index = byteIndex(id);
      if (arr.length <= index) {
        return false;
      }
      var v = arr[index];
      var mask = mask(id);
      if ((v & mask) != mask) {
        return false;
      }
    }
    return true;
  }

  private boolean containsMustMatchAny(Privilege privilege) {
    var arr = this.bytes;
    for (var p : privilege.resolved()) {
      var id = privileges.idForName(p.name());
      var index = byteIndex(id);
      if (arr.length <= index) {
        continue;
      }
      var v = arr[index];
      var mask = mask(id);
      if ((v & mask) == mask) {
        return true;
      }
    }
    return false;
  }

  @Override
  @SuppressWarnings("PatternMatchingInstanceof")
  public boolean containsAll(@Nonnull Collection<?> privileges) {
    for (var o : privileges) {
      if (!(o instanceof Privilege)) {
        return false;
      }
      if (!contains((Privilege) o)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean containsAny(Iterable<? extends Privilege> privileges) {
    for (var o : privileges) {
      if (contains(o)) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public Iterator<Privilege> iterator() {
    return iterator(privileges);
  }

  @Override
  public Iterator<Privilege> iterator(Privileges privileges) {
    return new AbstractIterator<>() {
      private int idx = 0;

      @Override
      protected Privilege computeNext() {
        while (true) {
          var i = idx++;
          var arrIdx = byteIndex(i);
          if (arrIdx >= PrivilegeSetImpl.this.bytes.length) {
            return endOfData();
          }
          var mask = mask(i);
          var v = PrivilegeSetImpl.this.bytes[arrIdx];
          if ((v & mask) == mask) {
            return privileges.byId(i);
          }
        }
      }
    };
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  @SuppressWarnings("EqualsGetClass")
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PrivilegeSetImpl privilegeSet = (PrivilegeSetImpl) o;
    return Arrays.equals(privilegeSet.bytes, bytes);
  }

  @Nonnull
  @Override
  public String toString() {
    var id = 0;
    var sb = new StringBuilder("PrivilegeSet{");
    var first = true;
    for (var b : this.bytes) {
      for (int i = 0, m = 1; i < 8; i++, m <<= 1) {
        if ((b & m) != 0) {
          if (first) {
            first = false;
          } else {
            sb.append(',');
          }
          sb.append(id);
        }
        id++;
      }
    }
    return sb.append('}').toString();
  }

  static int byteIndex(int id) {
    return id >> 3;
  }

  static byte mask(int id) {
    return (byte) (1 << (id & 7));
  }

  static PrivilegeSetBuilder builder(Privileges privileges) {
    return new PrivilegeSetBuilderImpl(privileges);
  }

  byte[] bytesUnsafe() {
    return bytes;
  }

  static final class PrivilegeSetBuilderImpl implements PrivilegeSetBuilder {
    private final BitSet bitSet;
    private final Privileges privileges;

    private PrivilegeSetBuilderImpl(Privileges privileges) {
      this.bitSet = new BitSet();
      this.privileges = privileges;
    }

    @Override
    public PrivilegeSetBuilder addPrivileges(@Nonnull Iterable<? extends Privilege> privileges) {
      for (var privilege : privileges) {
        addPrivilege(privilege);
      }
      return this;
    }

    @Override
    public PrivilegeSetBuilder addPrivileges(@Nonnull Privilege... privileges) {
      for (var privilege : privileges) {
        addPrivilege(privilege);
      }
      return this;
    }

    @Override
    public PrivilegeSetBuilder addPrivileges(@Nonnull PrivilegeSet privilegeSet) {
      var bytes =
          (privilegeSet instanceof PrivilegeSetImpl privilegeSetImpl)
              ? privilegeSetImpl.bytes
              : privilegeSet.toByteArray();
      // TODO `valueOf(byte[])` is way more expensive than `valueOf(long[])`
      bitSet.or(BitSet.valueOf(bytes));
      return this;
    }

    @Override
    public PrivilegeSetBuilder addPrivilege(@Nonnull Privilege privilege) {
      for (var individualPrivilege : privilege.resolved()) {
        var id = privileges.idForName(individualPrivilege.name());
        this.bitSet.set(id);
      }
      return this;
    }

    @Override
    public PrivilegeSetBuilder removePrivileges(@Nonnull Iterable<? extends Privilege> privileges) {
      for (var privilege : privileges) {
        removePrivilege(privilege);
      }
      return this;
    }

    @Override
    public PrivilegeSetBuilder removePrivileges(@Nonnull Privilege... privileges) {
      for (var privilege : privileges) {
        removePrivilege(privilege);
      }
      return this;
    }

    @Override
    public PrivilegeSetBuilder removePrivileges(@Nonnull PrivilegeSet privilegeSet) {
      var bytes =
          (privilegeSet instanceof PrivilegeSetImpl privilegeSetImpl)
              ? privilegeSetImpl.bytes
              : privilegeSet.toByteArray();
      // TODO `valueOf(byte[])` is way more expensive than `valueOf(long[])`
      bitSet.andNot(BitSet.valueOf(bytes));
      return this;
    }

    @Override
    public PrivilegeSetBuilder removePrivilege(@Nonnull Privilege privilege) {
      for (var individualPrivilege : privilege.resolved()) {
        var id = privileges.idForName(individualPrivilege.name());
        this.bitSet.clear(id);
      }
      return this;
    }

    @Override
    public PrivilegeSet build() {
      return new PrivilegeSetImpl(privileges, this);
    }
  }
}

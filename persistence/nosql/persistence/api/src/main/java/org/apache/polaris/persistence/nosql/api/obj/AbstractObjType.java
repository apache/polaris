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
package org.apache.polaris.persistence.nosql.api.obj;

import java.util.function.LongSupplier;

public abstract class AbstractObjType<T extends Obj> implements ObjType {
  private final String id;
  private final String name;
  private final Class<T> targetClass;

  protected AbstractObjType(String id, String name, Class<T> targetClass) {
    this.id = id;
    this.name = name;
    this.targetClass = targetClass;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Class<T> targetClass() {
    return targetClass;
  }

  public abstract static class AbstractUncachedObjType<T extends Obj> extends AbstractObjType<T> {
    protected AbstractUncachedObjType(String id, String name, Class<T> targetClass) {
      super(id, name, targetClass);
    }

    @Override
    public long cachedObjectExpiresAtMicros(Obj obj, LongSupplier clockMicros) {
      return 0L;
    }
  }

  @Override
  @SuppressWarnings("EqualsGetClass") // explict class-instance-equals is intentional
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AbstractObjType<?> that = (AbstractObjType<?>) o;

    return id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return id;
  }
}

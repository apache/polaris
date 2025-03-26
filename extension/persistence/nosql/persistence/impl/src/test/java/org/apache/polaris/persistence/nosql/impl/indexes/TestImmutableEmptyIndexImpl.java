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
package org.apache.polaris.persistence.nosql.impl.indexes;

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.emptyImmutableIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;

import java.nio.ByteBuffer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestImmutableEmptyIndexImpl {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void immutableEmpty() {
    var index = emptyImmutableIndex(OBJ_REF_SERIALIZER);

    var commitOp = Util.randomObjId();

    soft.assertThat(index.isLoaded()).isTrue();
    soft.assertThat(index.isModified()).isFalse();
    soft.assertThat(index.first()).isNull();
    soft.assertThat(index.last()).isNull();
    soft.assertThat(index.estimatedSerializedSize()).isEqualTo(2);
    soft.assertThat(index.serialize()).isEqualTo(ByteBuffer.wrap(new byte[] {(byte) 1, (byte) 0}));
    soft.assertThat(deserializeStoreIndex(index.serialize(), OBJ_REF_SERIALIZER).asKeyList())
        .isEqualTo(index.asKeyList());
    soft.assertThat(index.asKeyList()).isEmpty();
    soft.assertThat(index.stripes()).isEmpty();
    soft.assertThatThrownBy(() -> index.add(indexElement(key("foo"), commitOp)))
        .isInstanceOf(UnsupportedOperationException.class);
    soft.assertThatThrownBy(() -> index.remove(key("foo")))
        .isInstanceOf(UnsupportedOperationException.class);
    soft.assertThat(index.getElement(key("foo"))).isNull();
    soft.assertThat(index.containsElement(key("foo"))).isFalse();
    soft.assertThat(index.iterator(null, null, false)).isExhausted();
  }

  @Test
  public void stateRelated() {
    var index = emptyImmutableIndex(OBJ_REF_SERIALIZER);

    soft.assertThat(index.asMutableIndex()).isNotSameAs(index);
    soft.assertThat(index.isMutable()).isFalse();
    soft.assertThatThrownBy(() -> index.divide(3))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void serialization() {
    var index = emptyImmutableIndex(OBJ_REF_SERIALIZER);
    var mutable = newStoreIndex(OBJ_REF_SERIALIZER);
    soft.assertThat(index.serialize()).isEqualTo(mutable.serialize());
  }
}

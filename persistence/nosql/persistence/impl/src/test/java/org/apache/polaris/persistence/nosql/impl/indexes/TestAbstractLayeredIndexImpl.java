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

import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.layeredIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.lazyStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.KeyIndexTestSet.basicIndexTestSet;
import static org.apache.polaris.persistence.nosql.impl.indexes.Util.randomObjId;
import static org.assertj.core.util.Lists.newArrayList;

import java.util.ArrayList;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestAbstractLayeredIndexImpl {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void isModifiedReflected() {
    var reference = basicIndexTestSet().keyIndex();
    soft.assertThat(reference.isModified()).isFalse();

    var updates = newStoreIndex(OBJ_REF_SERIALIZER);
    for (var c = 'a'; c <= 'z'; c++) {
      updates.add(indexElement(key(c + "foo"), randomObjId()));
    }
    var layered = layeredIndex(reference, updates);
    soft.assertThat(updates.isModified()).isTrue();
    soft.assertThat(layered.isModified()).isTrue();

    updates = deserializeStoreIndex(updates.serialize(), OBJ_REF_SERIALIZER);
    layered = layeredIndex(reference, updates);
    soft.assertThat(updates.isModified()).isFalse();
    soft.assertThat(layered.isModified()).isFalse();

    reference.add(indexElement(key("foobar"), randomObjId()));
    soft.assertThat(reference.isModified()).isTrue();
    soft.assertThat(updates.isModified()).isFalse();
    soft.assertThat(layered.isModified()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void isLoadedReflected(boolean updateReference) {
    var reference = basicIndexTestSet().keyIndex();
    var lazyReference = lazyStoreIndex(() -> reference, null, null);
    soft.assertThat(lazyReference.isLoaded()).isFalse();

    var updates = newStoreIndex(OBJ_REF_SERIALIZER);
    var lazyUpdates = lazyStoreIndex(() -> updates, null, null);
    soft.assertThat(lazyUpdates.isLoaded()).isFalse();

    var layered = layeredIndex(lazyReference, lazyUpdates);
    soft.assertThat(layered.isLoaded()).isFalse();

    if (updateReference) {
      lazyReference.add(indexElement(key("abc"), randomObjId()));
      soft.assertThat(lazyReference.isLoaded()).isTrue();
      soft.assertThat(lazyUpdates.isLoaded()).isFalse();
    } else {
      lazyUpdates.add(indexElement(key("abc"), randomObjId()));
      soft.assertThat(lazyReference.isLoaded()).isFalse();
      soft.assertThat(lazyUpdates.isLoaded()).isTrue();
    }
  }

  @Test
  public void basicLayered() {
    var indexTestSet = basicIndexTestSet();
    var reference = indexTestSet.keyIndex();
    for (var k : indexTestSet.keys()) {
      var el = requireNonNull(reference.getElement(k)).getValue();
      reference.put(k, objRef(el.type(), el.id(), 1));
    }

    var expected = new ArrayList<IndexElement<ObjRef>>();
    var embedded = newStoreIndex(OBJ_REF_SERIALIZER);
    reference
        .elementIterator()
        .forEachRemaining(
            el -> {
              if ((expected.size() % 5) == 0) {
                el =
                    indexElement(el.getKey(), objRef(el.getValue().type(), ~el.getValue().id(), 1));
                embedded.add(el);
              }
              expected.add(el);
            });

    var layered = layeredIndex(reference, embedded);

    soft.assertThat(newArrayList(layered.elementIterator())).containsExactlyElementsOf(expected);
    soft.assertThat(newArrayList(layered.reverseElementIterator()))
        .containsExactlyElementsOf(expected.reversed());
    soft.assertThat(layered.asKeyList()).containsExactlyElementsOf(reference.asKeyList());

    soft.assertThat(layered.stripes()).containsExactly(layered);

    var referenceFirst = reference.first();
    var referenceLast = reference.last();
    soft.assertThat(referenceFirst).isNotNull();
    soft.assertThat(referenceLast).isNotNull();
    soft.assertThat(layered.first()).isEqualTo(referenceFirst);
    soft.assertThat(layered.last()).isEqualTo(referenceLast);

    for (var i = 0; i < expected.size(); i++) {
      var el = expected.get(i);

      soft.assertThat(layered.containsElement(el.getKey())).isTrue();
      soft.assertThat(layered.getElement(el.getKey())).isEqualTo(el);
      soft.assertThat(newArrayList(layered.elementIterator(el.getKey(), el.getKey(), false)))
          .allMatch(elem -> elem.getKey().startsWith(el.getKey()));

      soft.assertThat(newArrayList(layered.elementIterator(el.getKey(), null, false)))
          .containsExactlyElementsOf(expected.subList(i, expected.size()));
      soft.assertThat(newArrayList(layered.elementIterator(null, el.getKey(), false)))
          .containsExactlyElementsOf(expected.subList(0, i + 1));

      soft.assertThat(newArrayList(layered.reverseElementIterator(el.getKey(), null, false)))
          .containsExactlyElementsOf(expected.subList(i, expected.size()).reversed());
      soft.assertThat(newArrayList(layered.reverseElementIterator(null, el.getKey(), false)))
          .containsExactlyElementsOf(expected.subList(0, i + 1).reversed());
    }

    var veryFirst = indexElement(key("aaaaaaaaaa"), randomObjId());
    embedded.add(veryFirst);

    soft.assertThat(layered.containsElement(veryFirst.getKey())).isTrue();
    soft.assertThat(layered.getElement(veryFirst.getKey())).isEqualTo(veryFirst);
    expected.addFirst(veryFirst);
    soft.assertThat(newArrayList(layered.elementIterator())).containsExactlyElementsOf(expected);
    soft.assertThat(newArrayList(layered.reverseElementIterator()))
        .containsExactlyElementsOf(expected.reversed());
    soft.assertThat(layered.asKeyList().size()).isEqualTo(reference.asKeyList().size() + 1);

    soft.assertThat(layered.first()).isEqualTo(veryFirst.getKey());
    soft.assertThat(layered.last()).isEqualTo(referenceLast);

    var veryLast = indexElement(key("zzzzzzzzz"), randomObjId());
    embedded.add(veryLast);

    soft.assertThat(layered.containsElement(veryLast.getKey())).isTrue();
    soft.assertThat(layered.getElement(veryLast.getKey())).isEqualTo(veryLast);
    expected.add(veryLast);
    soft.assertThat(newArrayList(layered.elementIterator())).containsExactlyElementsOf(expected);
    soft.assertThat(newArrayList(layered.reverseElementIterator()))
        .containsExactlyElementsOf(expected.reversed());
    soft.assertThat(layered.asKeyList().size()).isEqualTo(reference.asKeyList().size() + 2);

    soft.assertThat(layered.first()).isEqualTo(veryFirst.getKey());
    soft.assertThat(layered.last()).isEqualTo(veryLast.getKey());
  }

  @Test
  public void firstLastEstimated() {
    var index1 = newStoreIndex(OBJ_REF_SERIALIZER);
    var index2 = newStoreIndex(OBJ_REF_SERIALIZER);
    var index3 = newStoreIndex(OBJ_REF_SERIALIZER);
    index1.add(indexElement(key("aaa"), randomObjId()));
    index2.add(indexElement(key("bbb"), randomObjId()));

    soft.assertThat(layeredIndex(index1, index2).first()).isEqualTo(index1.first());
    soft.assertThat(layeredIndex(index2, index1).first()).isEqualTo(index1.first());
    soft.assertThat(layeredIndex(index1, index2).last()).isEqualTo(index2.first());
    soft.assertThat(layeredIndex(index2, index1).last()).isEqualTo(index2.first());

    soft.assertThat(layeredIndex(index1, index3).first()).isEqualTo(index1.first());
    soft.assertThat(layeredIndex(index3, index1).first()).isEqualTo(index1.first());
    soft.assertThat(layeredIndex(index1, index3).last()).isEqualTo(index1.first());
    soft.assertThat(layeredIndex(index3, index1).last()).isEqualTo(index1.first());

    soft.assertThat(layeredIndex(index3, index1).estimatedSerializedSize())
        .isEqualTo(index3.estimatedSerializedSize() + index1.estimatedSerializedSize());
    soft.assertThat(layeredIndex(index1, index3).estimatedSerializedSize())
        .isEqualTo(index3.estimatedSerializedSize() + index1.estimatedSerializedSize());
    soft.assertThat(layeredIndex(index2, index1).estimatedSerializedSize())
        .isEqualTo(index2.estimatedSerializedSize() + index1.estimatedSerializedSize());
    soft.assertThat(layeredIndex(index1, index2).estimatedSerializedSize())
        .isEqualTo(index2.estimatedSerializedSize() + index1.estimatedSerializedSize());
  }

  @Test
  public void stateRelated() {
    var index1 = newStoreIndex(OBJ_REF_SERIALIZER);
    var index2 = newStoreIndex(OBJ_REF_SERIALIZER);
    var layered = layeredIndex(index1, index2);

    soft.assertThatThrownBy(layered::asMutableIndex)
        .isInstanceOf(UnsupportedOperationException.class);
    soft.assertThat(layered.isMutable()).isFalse();
    soft.assertThatThrownBy(() -> layered.divide(3))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void unsupported() {
    var index1 = newStoreIndex(OBJ_REF_SERIALIZER);
    var index2 = newStoreIndex(OBJ_REF_SERIALIZER);
    var layered = layeredIndex(index1, index2);

    soft.assertThatThrownBy(layered::serialize).isInstanceOf(UnsupportedOperationException.class);
    soft.assertThatThrownBy(() -> layered.add(indexElement(key("aaa"), randomObjId())))
        .isInstanceOf(UnsupportedOperationException.class);
    soft.assertThatThrownBy(() -> layered.remove(key("aaa")))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}

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
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.layeredIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.Util.randomObjId;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestReadOnlyLayeredIndexImpl {
  @InjectSoftAssertions SoftAssertions soft;

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

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
package org.apache.polaris.persistence.nosql.maintenance.impl;

import static java.util.Map.entry;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.PrimitiveSink;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.maintenance.impl.ScanHandler.RetainCheck;
import org.jspecify.annotations.NonNull;

/**
 * Collects reference names and objects to retain.
 *
 * <p>The implementation uses bloom-filters to limit the heap usage when a huge number of
 * references/objects is being used.
 */
@SuppressWarnings("UnstableApiUsage")
final class AllRetained {

  /**
   * Some "salt" to make the bloom filters non-deterministic, in case there are false-positives, to
   * reduce the number of false positives over time.
   */
  private final int salt;

  // @NonNull is the jspecify variant, which allows type-usage.
  // Jakarta's @Nonnull does not allow type-usage.
  private final BloomFilter<Map.@NonNull Entry<String, String>> refsFilter;
  private final BloomFilter<Map.@NonNull Entry<String, Long>> objsFilter;
  private long refAdds;
  private long objAdds;

  AllRetained(long expectedReferenceCount, long expectedObjCount, double fpp, int salt) {
    this.salt = salt;
    this.refsFilter = BloomFilter.create(this::refFunnel, expectedReferenceCount, fpp);
    this.objsFilter = BloomFilter.create(this::objFunnel, expectedObjCount, fpp);
  }

  private void refFunnel(Map.Entry<String, String> realmRef, @Nonnull PrimitiveSink primitiveSink) {
    primitiveSink.putInt(salt);
    primitiveSink.putUnencodedChars(realmRef.getKey());
    primitiveSink.putUnencodedChars(realmRef.getValue());
  }

  private void objFunnel(Map.Entry<String, Long> realmObj, @Nonnull PrimitiveSink primitiveSink) {
    primitiveSink.putInt(salt);
    primitiveSink.putUnencodedChars(realmObj.getKey());
    var id = realmObj.getValue();
    primitiveSink.putLong(id);
  }

  void addRetainedRef(String realm, String ref) {
    refsFilter.put(entry(realm, ref));
    refAdds++;
  }

  void addRetainedObj(String realm, long id) {
    objsFilter.put(entry(realm, id));
    objAdds++;
  }

  /** The number of {@link #addRetainedRef(String, String)} invocations. */
  long refAdds() {
    return refAdds;
  }

  /** The number of {@link #addRetainedObj(String, long)} invocations. */
  long objAdds() {
    return objAdds;
  }

  boolean withinExpectedFpp(double expectedFpp) {
    return refsFilter.expectedFpp() < expectedFpp && objsFilter.expectedFpp() < expectedFpp;
  }

  RetainCheck<String> referenceRetainCheck() {
    return (realm, ref) -> refsFilter.mightContain(entry(realm, ref));
  }

  RetainCheck<ObjRef> objRetainCheck(Predicate<String> objTypeIdPredicate) {
    return (realm, id) ->
        objTypeIdPredicate.test(id.type()) || objsFilter.mightContain(entry(realm, id.id()));
  }
}

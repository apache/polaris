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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.binarySearch;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.deserializeKey;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.agrona.collections.Hashing;
import org.agrona.collections.Long2ObjectHashMap;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

/**
 * Implementation of {@link Index} that implements "version 1 serialization" of key-index-segments.
 *
 * <p>"Version 1" uses a diff-like encoding to compress keys and a custom var-int encoding. {@link
 * IndexElement}s are serialized in their natural order.
 *
 * <p>{@link IndexKey}s are serialized by serializing each element's UTF-8 representation with a
 * terminating {@code 0} byte, and the whole key terminated by a trailing {@code 0} byte. Empty key
 * elements are not allowed. The total serialized size of a key must not exceed {@value
 * #MAX_KEY_BYTES}.
 *
 * <p>Key serialization considers the previously serialized key - common prefixes are not
 * serialized. One var-ints is used to implement a diff-ish encoding: The var-int represents the
 * number of trailing bytes to strip from the previous key, then all bytes until the
 * double-zero-bytes end-marker is appended. For example, if the previous key was {@code
 * aaa.bbb.TableFoo} and the "current" key is {@code aaa.bbb.TableBarBaz}, the last three bytes of
 * {@code aaa.bbb.TableFoo} need to be removed, resulting in {@code aaa.bbb.Table} and 6 more bytes
 * ({@code BarBaz}) need to be appended to form the next key {@code aaa.bbb.TableBarBaz}. In this
 * case, the var-int {@code 3} is written plus the serialized representation of the 6 bytes to
 * represent {@code BarBaz} are serialized. The first serialized key is written in its entirety,
 * omitting the var-int that represents the number of bytes to "strip" from the previous key.
 *
 * <p>Using var-ints to represent the number of bytes to "strip" from the previous key is more space
 * efficient. It is very likely that two keys serialized after each other share a long common
 * prefix, especially since keys are serialized in their natural order.
 *
 * <p>The serialized key-index does not write any length information of the individual elements or
 * parts (like the {@link IndexKey} or value) to reduce the space required for serialization.
 *
 * <h2>Other ideas</h2>
 *
 * <p>There are other possible ideas and approaches to implement a serializable index of {@link
 * IndexKey} to something else:
 *
 * <ul>
 *   <li>Assumption (not true): Store serialized keys <em>separate</em> from other binary content,
 *       assuming that {@link IndexKey}s are compressible and the compression ratio of a set of keys
 *       is pretty good, unlike for example hash values, which are rather random and serialization
 *       likely does not benefit from compression.
 *       <p><em>RESULT</em> Experiment with >80000 words (each at least 10 chars long) for key
 *       elements: compression (gzip) of a key-to-commit-entry index (32 byte hashes) with
 *       interleaved key and value saves about 15% - the compressed ratio with keys first is only
 *       marginally better (approx 20%), so it is not worth the extra complexity.
 *   <li>Assumption (not true): Compressing the key-indexes helps with reducing database round trips
 *       a lot. As mentioned above, the savings of compression are around 15-22%. We can assume that
 *       the network traffic to the database is already compressed, so we do not save bandwidth - it
 *       might save one (or two) row reads of a bulk read. The savings do not feel worth the extra
 *       complexity.
 *   <li>Have another implementation that is similar to this one, but uses a {@link
 *       java.util.TreeMap} to build indexes, when there are many elements to add to the index
 *   <li>Cross-check whether the left-truncation used in the serialized representation of this
 *       implementation is really legit in real life. <em>It still feels valid and legit and
 *       efficient.</em>
 *   <li>Add some checksum (e.g. {@link java.util.zip.CRC32C}, preferred, or {@link
 *       java.util.zip.CRC32}) to the serialized representation?
 * </ul>
 */
final class IndexImpl<V> implements IndexSpi<V> {

  static final int MAX_KEY_BYTES = 4096;

  /**
   * Assume 4 additional bytes for each added entry: 2 bytes for the "strip" and 2 bytes for the
   * "add" var-ints.
   */
  private static final int ASSUMED_PER_ENTRY_OVERHEAD = 2 + 2;

  private static final byte CURRENT_STORE_INDEX_VERSION = 1;

  public static final Comparator<IndexElement<?>> KEY_COMPARATOR =
      Comparator.comparing(IndexElement::getKey);

  /**
   * Serialized size of the index at the time when the {@link #IndexImpl(List, int,
   * IndexValueSerializer, boolean)} constructor has been called.
   *
   * <p>This field is used to <em>estimate</em> the serialized size when this object is serialized
   * again, including modifications.
   */
  private final int originalSerializedSize;

  private int estimatedSerializedSizeDiff;
  private final List<IndexElement<V>> elements;
  private final IndexValueSerializer<V> serializer;

  /**
   * Buffer that holds the raw serialized value of a store index. This buffer's {@link
   * ByteBuffer#position()} and {@link ByteBuffer#limit()} are updated by the users of this buffer
   * to perform the necessary operations. Note: {@link IndexImpl} is not thread safe as defined by
   * {@link Index}
   */
  private final ByteBuffer serialized;

  /**
   * Used to drastically reduce the amount of 4k {@link ByteBuffer} allocations during key
   * deserialization operations, which are very frequent. The JMH results alone would justify the
   * use of a {@link ThreadLocal} in this case. However, those would add a "permanent GC root" to
   * this class. The implemented approach is a bit more expensive, but without the drawbacks of
   * {@link ThreadLocal}s.
   *
   * <p>An implementation based on an {@link java.util.ArrayDeque} was discarded, because it is way
   * too expensive.
   *
   * <p>JMH results are as follows. Invoked via {@code java -jar
   * persistence/nosql/persistence/impl/build/libs/polaris-persistence-nosql-impl-1.1.0-incubating-SNAPSHOT-jmh.jar
   * RealisticKeyIndexImplBench -p namespaceLevels=3 -p foldersPerLevel=5 -p tablesPerNamespace=5
   * -prof gc -prof perf}.
   *
   * <table>
   *     <tr>
   *         <th>Benchmark</th>
   *         <th><code>Deque&lt;ByteBuffer&gt;</code></th>
   *         <th><code>ThreadLocal&lt;ByteBuffer&gt;</code></th>
   *         <th><code>Long2ObjectHashMap</code></th>
   *         <th>unit</th>
   *     </tr>
   *     <tr>
   *         <td>RealisticKeyIndexImplBench.deserializeAdd</td>
   *         <td>90</td>
   *         <td>22</td>
   *         <td>25</td>
   *         <td>µs/op (lower is better)</td>
   *     </tr>
   *     <tr>
   *         <td>RealisticKeyIndexImplBench.deserializeAdd</td>
   *         <td>500k</td>
   *         <td>100k</td>
   *         <td>110k</td>
   *         <td>bytes/op (lower is better)</td>
   *     </tr>
   *     <tr>
   *         <td>RealisticKeyIndexImplBench.deserializeAdd</td>
   *         <td>1.2</td>
   *         <td>4.5</td>
   *         <td>4.2</td>
   *         <td>insn/clk (higher is better)</td>
   *     </tr>
   *     <tr>
   *         <td>RealisticKeyIndexImplBench.deserializeAddSerialize</td>
   *         <td>100</td>
   *         <td>45</td>
   *         <td>50</td>
   *         <td>µs/op (lower is better)</td>
   *     </tr>
   *     <tr>
   *         <td>RealisticKeyIndexImplBench.deserializeAddSerialize</td>
   *         <td>530k</td>
   *         <td>130k</td>
   *         <td>130k</td>
   *         <td>bytes/op (lower is better)</td>
   *     </tr>
   *     <tr>
   *         <td>RealisticKeyIndexImplBench.deserializeAddSerialize</td>
   *         <td>2.3</td>
   *         <td>5.0</td>
   *         <td>4.7</td>
   *         <td>insn/clk (higher is better)</td>
   *     </tr>
   * </table>
   */
  private static final class ScratchBuffer {
    final ByteBuffer buffer;
    volatile long lastUsed;

    ScratchBuffer() {
      this.buffer = newKeyBuffer();
    }
  }

  /**
   * Maximum number of cached {@link ByteBuffer}s, equals to the number of active threads accessing
   * indexes.
   */
  private static final int MAX_KEY_BUFFERS = 2048;

  private static final Long2ObjectHashMap<ScratchBuffer> SCRATCH_KEY_BUFFERS =
      new Long2ObjectHashMap<>(256, Hashing.DEFAULT_LOAD_FACTOR, true);

  private static ByteBuffer scratchKeyBuffer() {
    var tid = Thread.currentThread().threadId();
    var t = System.nanoTime();
    synchronized (SCRATCH_KEY_BUFFERS) {
      var buffer = SCRATCH_KEY_BUFFERS.get(tid);
      if (buffer == null) {
        buffer = new ScratchBuffer();
        if (SCRATCH_KEY_BUFFERS.size() == MAX_KEY_BUFFERS) {
          var maxAge = Long.MAX_VALUE;
          var candidate = -1L;
          for (var iter = SCRATCH_KEY_BUFFERS.entrySet().iterator(); iter.hasNext(); ) {
            iter.next();
            var b = iter.getValue();
            var age = Math.max(t - b.lastUsed, 0L);
            if (age < maxAge) {
              candidate = iter.getLongKey();
              maxAge = age;
            }
          }
          // Intentionally remove (evict) the youngest one, as its more likely that old scratch
          // buffers are in an "old GC generation", which is more costly to garbage collect.
          SCRATCH_KEY_BUFFERS.remove(candidate);
        }
        SCRATCH_KEY_BUFFERS.put(tid, buffer);
        buffer.lastUsed = t;
      } else {
        buffer.buffer.clear();
      }
      return buffer.buffer;
    }
  }

  private boolean modified;
  private ObjRef objRef;

  // NOTE: The implementation uses j.u.ArrayList to optimize for reads. Additions to this data
  // structure are rather inefficient when elements need to be added "in the middle" of the
  // 'elements' j.u.ArrayList.

  IndexImpl(IndexValueSerializer<V> serializer) {
    this(new ArrayList<>(), 2, serializer, false);
  }

  private IndexImpl(
      List<IndexElement<V>> elements,
      int originalSerializedSize,
      IndexValueSerializer<V> serializer,
      boolean modified) {
    this.elements = elements;
    this.originalSerializedSize = originalSerializedSize;
    this.serializer = serializer;
    this.modified = modified;
    this.serialized = null;
  }

  @Override
  public boolean isModified() {
    return modified;
  }

  @VisibleForTesting
  IndexImpl<V> setModified() {
    modified = true;
    return this;
  }

  @Override
  public ObjRef getObjId() {
    return objRef;
  }

  @Override
  public IndexSpi<V> setObjId(ObjRef objRef) {
    this.objRef = objRef;
    return this;
  }

  @Override
  public void prefetchIfNecessary(Iterable<IndexKey> keys) {}

  @Override
  public boolean isLoaded() {
    return true;
  }

  @Override
  public IndexSpi<V> asMutableIndex() {
    return this;
  }

  @Override
  public boolean isMutable() {
    return true;
  }

  @Override
  public List<IndexSpi<V>> divide(int parts) {
    var elems = elements;
    var size = elems.size();
    checkArgument(
        parts > 0 && parts <= size,
        "Number of parts %s must be greater than 0 and less or equal to number of elements %s",
        parts,
        size);
    var partSize = size / parts;
    var serializedMax = originalSerializedSize + estimatedSerializedSizeDiff;

    var result = new ArrayList<IndexSpi<V>>(parts);
    var index = 0;
    for (var i = 0; i < parts; i++) {
      var end = i < parts - 1 ? index + partSize : elems.size();
      var partElements = new ArrayList<>(elements.subList(index, end));
      var part = new IndexImpl<>(partElements, serializedMax, serializer, true);
      result.add(part);
      index = end;
    }
    return result;
  }

  @Override
  public List<IndexSpi<V>> stripes() {
    return List.of(this);
  }

  @Override
  public IndexSpi<V> mutableStripeForKey(IndexKey key) {
    return this;
  }

  @Override
  public boolean hasElements() {
    return !elements.isEmpty();
  }

  @Override
  public boolean add(@Nonnull IndexElement<V> element) {
    modified = true;
    var e = elements;
    var serializer = this.serializer;
    var idx = search(e, element);
    var elementSerializedSize = element.contentSerializedSize(serializer);
    if (idx >= 0) {
      // exact match, key already in the segment
      var prev = e.get(idx);

      var prevSerializedSize = prev.contentSerializedSize(serializer);
      estimatedSerializedSizeDiff += elementSerializedSize - prevSerializedSize;

      e.set(idx, element);
      return false;
    }

    estimatedSerializedSizeDiff += addElementDiff(element, elementSerializedSize);

    var insertionPoint = -idx - 1;
    if (insertionPoint == e.size()) {
      e.add(element);
    } else {
      e.add(insertionPoint, element);
    }
    return true;
  }

  private static <V> int addElementDiff(IndexElement<V> element, int elementSerializedSize) {
    return element.getKey().serializedSize() + ASSUMED_PER_ENTRY_OVERHEAD + elementSerializedSize;
  }

  @Override
  public boolean remove(@Nonnull IndexKey key) {
    var e = elements;
    var idx = search(e, key);
    if (idx < 0) {
      return false;
    }

    modified = true;

    var element = e.remove(idx);

    estimatedSerializedSizeDiff -= removeSizeDiff(element);

    return true;
  }

  private int removeSizeDiff(IndexElement<V> element) {
    return 2 + element.contentSerializedSize(serializer);
  }

  @Override
  public boolean containsElement(@Nonnull IndexKey key) {
    var idx = search(elements, key);
    return idx >= 0;
  }

  @Override
  public boolean contains(@Nonnull IndexKey key) {
    var el = getElement(key);
    return el != null && el.getValue() != null;
  }

  @Override
  public @Nullable IndexElement<V> getElement(@Nonnull IndexKey key) {
    var e = elements;
    var idx = search(e, key);
    if (idx < 0) {
      return null;
    }
    return e.get(idx);
  }

  @Nullable
  @Override
  public IndexKey first() {
    var e = elements;
    return e.isEmpty() ? null : e.getFirst().getKey();
  }

  @Nullable
  @Override
  public IndexKey last() {
    var e = elements;
    return e.isEmpty() ? null : e.getLast().getKey();
  }

  @Override
  public @Nonnull Iterator<IndexElement<V>> elementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    var e = elements;

    if (lower == null && higher == null) {
      return e.iterator();
    }

    var prefix = lower != null && lower.equals(higher);
    var fromIdx = lower != null ? iteratorIndex(lower, 0) : 0;
    var toIdx = !prefix && higher != null ? iteratorIndex(higher, 1) : e.size();

    checkArgument(toIdx >= fromIdx, "'to' must be greater than 'from'");

    e = e.subList(fromIdx, toIdx);
    var base = e.iterator();
    return prefix
        ? new AbstractIterator<>() {

          @Override
          protected IndexElement<V> computeNext() {
            if (!base.hasNext()) {
              return endOfData();
            }
            var v = base.next();
            if (!v.getKey().startsWith(lower)) {
              return endOfData();
            }
            return v;
          }
        }
        : base;
  }

  @Override
  public @Nonnull Iterator<IndexElement<V>> reverseElementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    var e = elements;

    if (lower == null && higher == null) {
      return e.reversed().iterator();
    }

    var prefix = lower != null && lower.equals(higher);
    checkArgument(!prefix, "reverse prefix-queries are not supported");
    var fromIdx = higher != null ? iteratorIndex(higher, 1) : e.size();
    var toIdx = lower != null ? iteratorIndex(lower, 0) : 0;

    checkArgument(toIdx <= fromIdx, "'to' must be greater than 'from'");

    e = e.subList(toIdx, fromIdx).reversed();
    return e.iterator();
  }

  private int iteratorIndex(IndexKey from, int exactAdd) {
    var fromIdx = search(elements, from);
    if (fromIdx < 0) {
      fromIdx = -fromIdx - 1;
    } else {
      fromIdx += exactAdd;
    }
    return fromIdx;
  }

  @Override
  @VisibleForTesting
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IndexImpl)) {
      return false;
    }
    @SuppressWarnings("unchecked")
    var that = (IndexImpl<V>) o;
    return elements.equals(that.elements);
  }

  @Override
  @VisibleForTesting
  public int hashCode() {
    return elements.hashCode();
  }

  @Override
  public String toString() {
    var f = first();
    var l = last();
    var fk = f != null ? f.toString() : "";
    var lk = l != null ? l.toString() : "";
    return "IndexImpl{size=" + elements.size() + ", first=" + fk + ", last=" + lk + "}";
  }

  @Override
  public List<IndexKey> asKeyList() {
    return new AbstractList<>() {
      @Override
      public IndexKey get(int index) {
        return elements.get(index).getKey();
      }

      @Override
      public int size() {
        return elements.size();
      }
    };
  }

  @Override
  public int estimatedSerializedSize() {
    return originalSerializedSize + estimatedSerializedSizeDiff;
  }

  @Override
  public @Nonnull ByteBuffer serialize() {
    ByteBuffer target;

    if (serialized == null || modified) {
      var elements = this.elements;

      target = ByteBuffer.allocate(estimatedSerializedSize());

      // Serialized segment index version
      target.put(CURRENT_STORE_INDEX_VERSION);
      putVarInt(target, elements.size());

      ByteBuffer previousKey = null;

      var scratchKeyBuffer = scratchKeyBuffer();

      boolean onlyLazy;
      IndexElement<V> previous = null;
      for (var el : elements) {
        ByteBuffer keyBuf = null;
        if (isLazyElementImpl(el)) {
          var lazyEl = (LazyIndexElement) el;
          // The purpose of this 'if'-branch is to determine whether it can serialize the 'IndexKey'
          // by _not_ fully materializing the `IndexKey`. This is possible if (and only if!) the
          // current and the previous element are `LazyStoreIndexElement`s, where the previous
          // element is exactly the one that has been deserialized.
          //noinspection RedundantIfStatement
          if (lazyEl.prefixLen == 0 || lazyEl.previous == previous) {
            // Can use the optimized serialization in `LazyStoreIndexElement` if the current
            // element has no prefix of if the previously serialized element was also a
            // `LazyStoreIndexElement`. In other words, no intermediate `LazyStoreIndexElement` has
            // been removed and no new element has been added.
            onlyLazy = true;
          } else {
            // This if-branch detects whether an element has been removed from the index. In that
            // case, serialization has to materialize the `IndexKey` for serialization.
            onlyLazy = false;
          }
          if (onlyLazy) {
            // Key serialization via 'LazyStoreIndexElement' is much cheaper (CPU and heap) than
            // having to first materialize and then serialize it.
            keyBuf = lazyEl.serializeKey(scratchKeyBuffer, previousKey);
          }
        } else {
          onlyLazy = false;
        }

        if (!onlyLazy) {
          // Either 'el' is not a 'LazyStoreIndexElement' or the previous element of a
          // 'LazyStoreIndexElement' is not suitable (see above).
          keyBuf = serializeIndexKeyString(el.getKey(), scratchKeyBuffer);
        }

        previousKey = serializeKey(keyBuf, previousKey, target);
        el.serializeContent(serializer, target);
        previous = el;
      }

      target = target.flip();
    } else {
      target = serializedThreadSafe().position(0).limit(originalSerializedSize);
    }

    return target;
  }

  // IntelliJ warns "Condition 'el.getClass() == LazyStoreIndexElement.class' is always 'false'",
  // which is a false positive (see below as well).
  @SuppressWarnings("ConstantValue")
  private boolean isLazyElementImpl(IndexElement<V> el) {
    return el.getClass() == LazyIndexElement.class;
  }

  private ByteBuffer serializeKey(ByteBuffer keyBuf, ByteBuffer previousKey, ByteBuffer target) {
    var keyPos = keyBuf.position();
    if (previousKey != null) {
      var mismatch = previousKey.mismatch(keyBuf);
      checkState(mismatch != -1, "Previous and current keys must not be equal");
      var strip = previousKey.remaining() - mismatch;
      putVarInt(target, strip);
      keyBuf.position(keyPos + mismatch);
    } else {
      previousKey = newKeyBuffer();
    }
    target.put(keyBuf);

    previousKey.clear();
    keyBuf.position(keyPos);
    previousKey.put(keyBuf).flip();

    return previousKey;
  }

  static <V> IndexSpi<V> deserializeStoreIndex(ByteBuffer serialized, IndexValueSerializer<V> ser) {
    return new IndexImpl<>(serialized, ser);
  }

  /**
   * Private constructor handling deserialization, required to instantiate the inner {@link
   * LazyIndexElement} class.
   */
  private IndexImpl(ByteBuffer serialized, IndexValueSerializer<V> ser) {
    var version = serialized.get();
    checkArgument(version == 1, "Unsupported serialized representation of KeyIndexSegment");

    var elements = new ArrayList<IndexElement<V>>(readVarInt(serialized));

    var first = true;
    var previousKeyLen = 0;
    LazyIndexElement predecessor = null;
    LazyIndexElement previous = null;

    while (serialized.remaining() > 0) {
      var strip = first ? 0 : readVarInt(serialized);
      first = false;

      var prefixLen = previousKeyLen - strip;
      checkArgument(prefixLen >= 0, "prefixLen must be >= 0");
      var keyOffset = serialized.position();
      IndexKey.skip(serialized); // skip key
      var valueOffset = serialized.position();
      ser.skip(serialized); // skip content/value
      var endOffset = serialized.position();

      var keyPartLen = valueOffset - keyOffset;
      var totalKeyLen = prefixLen + keyPartLen;

      predecessor = cutPredecessor(predecessor, prefixLen, previous);

      // 'prefixLen==0' means that the current key represents the "full" key.
      // It has no predecessor that would be needed to re-construct (aka materialize) the full key.
      var elementPredecessor = prefixLen > 0 ? predecessor : null;
      var element =
          new LazyIndexElement(
              elementPredecessor, previous, keyOffset, prefixLen, valueOffset, endOffset);
      if (elementPredecessor == null) {
        predecessor = element;
      } else if (predecessor.prefixLen > prefixLen) {
        predecessor = element;
      }
      elements.add(element);

      previous = element;
      previousKeyLen = totalKeyLen;
    }

    this.elements = elements;
    this.serializer = ser;
    this.originalSerializedSize = serialized.position();
    this.serialized = serialized.duplicate().clear();
  }

  /**
   * Identifies the earliest suitable predecessor, which is a very important step during
   * deserialization, because otherwise the chain of predecessors (to the element having a {@code
   * prefixLen==0}) can easily become very long in the order of many thousands "hops", which makes
   * key materialization overly expensive.
   */
  private LazyIndexElement cutPredecessor(
      LazyIndexElement predecessor, int prefixLen, LazyIndexElement previous) {
    if (predecessor != null) {
      if (predecessor.prefixLen < prefixLen) {
        // If the current element's prefixLen is higher, let the current element's predecessor point
        // to the previous element.
        predecessor = previous;
      } else {
        // Otherwise, find the predecessor that has "enough" data. Without this step, the chain of
        // predecessors would become extremely long.
        for (var p = predecessor; ; p = p.predecessor) {
          if (p == null || p.prefixLen < prefixLen) {
            break;
          }
          predecessor = p;
        }
      }
    }
    return predecessor;
  }

  private final class LazyIndexElement extends AbstractIndexElement<V> {
    /**
     * Points to the predecessor (in index order) that has a required part of the index-key needed
     * to deserialize. In other words, if multiple index-elements have the same {@code prefixLen},
     * this one points to the first one (in index order), because referencing the "intermediate"
     * predecessors in-between would yield no part of the index-key to be re-constructed.
     *
     * <p>This fields holds the "earliest" predecessor in deserialization order, as determined by
     * {@link #cutPredecessor(LazyIndexElement, int, LazyIndexElement)}.
     *
     * <p>Example:<code><pre>
     *  IndexElement #0 { prefixLen = 0, key = "aaa", predecessor = null }
     *  IndexElement #1 { prefixLen = 2, key = "aab", predecessor = #0 }
     *  IndexElement #2 { prefixLen = 2, key = "aac", predecessor = #0 }
     *  IndexElement #3 { prefixLen = 1, key = "abb", predecessor = #0 }
     *  IndexElement #4 { prefixLen = 0, key = "bbb", predecessor = null }
     *  IndexElement #5 { prefixLen = 2, key = "bbc", predecessor = #4 }
     *  IndexElement #6 { prefixLen = 3, key = "bbcaaa", predecessor = #5 }
     * </pre></code>
     */
    final LazyIndexElement predecessor;

    /**
     * The previous element in the order of deserialization. This is needed later during
     * serialization.
     */
    final LazyIndexElement previous;

    /** Number of bytes for this element's key that are held by its predecessor(s). */
    final int prefixLen;

    /** Position in {@link IndexImpl#serialized} at which this index-element's key part starts. */
    final int keyOffset;

    /** Position in {@link IndexImpl#serialized} at which this index-element's value starts. */
    final int valueOffset;

    /**
     * Position in {@link #serialized} pointing to the first byte <em>after</em> this element's key
     * and value.
     */
    final int endOffset;

    /** The materialized key or {@code null}. */
    private IndexKey key;

    /** The materialized content or {@code null}. */
    private V content;

    private boolean hasContent;

    LazyIndexElement(
        LazyIndexElement predecessor,
        LazyIndexElement previous,
        int keyOffset,
        int prefixLen,
        int valueOffset,
        int endOffset) {
      this.predecessor = predecessor;
      this.previous = previous;
      this.keyOffset = keyOffset;
      this.prefixLen = prefixLen;
      this.valueOffset = valueOffset;
      this.endOffset = endOffset;
    }

    ByteBuffer serializeKey(ByteBuffer keySerBuffer, ByteBuffer previousKey) {
      keySerBuffer.clear();
      if (previousKey != null) {
        var limitSave = previousKey.limit();
        keySerBuffer.put(previousKey.limit(prefixLen).position(0));
        previousKey.limit(limitSave).position(0);
      }

      return keySerBuffer
          .put(serializedNotThreadSafe().limit(valueOffset).position(keyOffset))
          .flip();
    }

    private IndexKey materializeKey() {
      var serialized = serializedThreadSafe();

      var suffix = serialized.limit(valueOffset).position(keyOffset);

      var preLen = prefixLen;
      var keyBuffer =
          preLen > 0
              ? prefixKey(serialized, this, preLen).position(preLen).put(suffix).flip()
              : suffix;
      return deserializeKey(keyBuffer);
    }

    private ByteBuffer prefixKey(ByteBuffer serialized, LazyIndexElement me, int remaining) {
      var keyBuffer = scratchKeyBuffer();

      // This loop could be easier written using recursion. However, recursion is way more expensive
      // than this loop. Since this code is on a very hot code path, it is worth it.
      for (var e = me.predecessor; e != null; e = e.predecessor) {
        if (e.key != null) {
          // In case the current 'e' has its key already materialized, use that one to construct the
          // prefix for "our" key.
          var limitSave = keyBuffer.limit();
          try {
            // Call 'putString' with the parameter 'shortened==true' to instruct the function to
            // expect buffer overruns and handle those gracefully.
            e.key.serializeNoFail(keyBuffer.limit(remaining));
          } finally {
            keyBuffer.limit(limitSave);
          }
          break;
        }

        var prefixLen = e.prefixLen;
        var take = remaining - prefixLen;
        if (take > 0) {
          remaining -= take;

          for (int src = e.keyOffset, dst = e.prefixLen; take-- > 0; src++, dst++) {
            keyBuffer.put(dst, serialized.get(src));
          }
        }
      }

      return keyBuffer;
    }

    @Override
    public void serializeContent(IndexValueSerializer<V> ser, ByteBuffer target) {
      target.put(serializedNotThreadSafe().limit(endOffset).position(valueOffset));
    }

    @Override
    public int contentSerializedSize(IndexValueSerializer<V> ser) {
      return endOffset - valueOffset;
    }

    @Override
    public IndexKey getKey() {
      var k = key;
      if (k == null) {
        k = key = materializeKey();
      }
      return k;
    }

    @Override
    public V getValue() {
      var c = content;
      if (c == null) {
        if (!hasContent) {
          c =
              content =
                  serializer.deserialize(
                      serializedThreadSafe().limit(endOffset).position(valueOffset));
          hasContent = true;
        }
      }
      return c;
    }

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      var k = key;
      var c = content;
      if (k != null && c != null) {
        return super.toString();
      }

      var sb = new StringBuilder("LazyStoreIndexElement(");
      if (k != null) {
        sb.append("key=").append(k);
      } else {
        sb.append("keyOffset=").append(keyOffset).append(", prefixLen=").append(prefixLen);
      }

      if (c != null) {
        sb.append(", content=").append(c);
      } else {
        sb.append(", valueOffset=").append(valueOffset).append(" endOffset=").append(endOffset);
      }

      return sb.toString();
    }
  }

  @VisibleForTesting
  static ByteBuffer newKeyBuffer() {
    return ByteBuffer.allocate(MAX_KEY_BYTES);
  }

  /**
   * Non-thread-safe variant to retrieve {@link #serialized}. Used in these scenarios, which are not
   * thread safe by contract:
   *
   * <ul>
   *   <li>serializing a <em>modified</em>
   * </ul>
   */
  private ByteBuffer serializedNotThreadSafe() {
    return requireNonNull(serialized);
  }

  /**
   * Thread-safe variant to retrieve {@link #serialized}. Used in these scenarios, which are not
   * thread safe by contract:
   *
   * <ul>
   *   <li>serializing a non-modified index
   *   <li>lazy materialization of a key (deserialization)
   *   <li>lazy materialization of a value (deserialization)
   * </ul>
   */
  private ByteBuffer serializedThreadSafe() {
    return requireNonNull(serialized).duplicate();
  }

  private static <V> int search(List<IndexElement<V>> e, @Nonnull IndexKey key) {
    // Need a StoreIndexElement for the sake of 'binarySearch()' (the content value isn't used)
    return search(e, indexElement(key, ""));
  }

  private static <V> int search(List<IndexElement<V>> e, IndexElement<?> element) {
    return binarySearch(e, element, KEY_COMPARATOR);
  }

  static ByteBuffer serializeIndexKeyString(IndexKey key, ByteBuffer keySerializationBuffer) {
    keySerializationBuffer.clear();
    try {
      return key.serialize(keySerializationBuffer).flip();
    } catch (BufferOverflowException e) {
      throw new IllegalArgumentException("Serialized key too big");
    }
  }
}

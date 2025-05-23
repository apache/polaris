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

import static java.lang.Math.pow;
import static java.util.UUID.randomUUID;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;

import com.google.common.collect.Sets;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj;
import org.assertj.core.util.Preconditions;
import org.immutables.value.Value;

/**
 * Generates a configurable set {@link IndexKey}s and test helper functionality to de-serialize
 * indexes using this set of keys.
 */
@PolarisImmutable
public interface KeyIndexTestSet<ELEMENT> {

  static KeyIndexTestSet<ObjRef> basicIndexTestSet() {
    var idGen = new AtomicLong();
    return KeyIndexTestSet.<ObjRef>newGenerator()
        .elementSupplier(
            key -> indexElement(key, objRef(SimpleTestObj.TYPE, idGen.incrementAndGet(), 1)))
        .elementSerializer(OBJ_REF_SERIALIZER)
        .build()
        .generateIndexTestSet();
  }

  @Value.Parameter(order = 1)
  List<IndexKey> keys();

  @Value.Parameter(order = 2)
  ByteBuffer serialized();

  default ByteBuffer serializedSafe() {
    return serialized().duplicate();
  }

  @Value.Parameter(order = 3)
  IndexSpi<ELEMENT> keyIndex();

  @Value.Parameter(order = 4)
  IndexSpi<ELEMENT> sourceKeyIndex();

  static <ELEMENT> KeyIndexTestSet<ELEMENT> of(
      List<IndexKey> keys,
      ByteBuffer serialized,
      IndexSpi<ELEMENT> keyIndex,
      IndexSpi<ELEMENT> sourceKeyIndex) {
    return ImmutableKeyIndexTestSet.of(keys, serialized, keyIndex, sourceKeyIndex);
  }

  static <ELEMENT> ImmutableIndexTestSetGenerator.Builder<ELEMENT> newGenerator() {
    return ImmutableIndexTestSetGenerator.builder();
  }

  @FunctionalInterface
  interface KeySet {
    List<IndexKey> keys();
  }

  /**
   * Generates {@link IndexKey}s consisting of a single element from the string representation of
   * random UUIDs.
   */
  @PolarisImmutable
  abstract class RandomUuidKeySet implements KeySet {
    @Value.Default
    public int numKeys() {
      return 1000;
    }

    @Override
    public List<IndexKey> keys() {
      Set<IndexKey> keys = new TreeSet<>();
      for (int i = 0; i < numKeys(); i++) {
        keys.add(key(randomUUID().toString()));
      }
      return new ArrayList<>(keys);
    }
  }

  /**
   * Generates {@link IndexKey}s based on realistic name patterns using a configurable amount of
   * namespace levels, namespaces per level and tables per namespace. Key elements are derived from
   * a set of more than 80000 words, each at least 10 characters long. The {@link #deterministic()}
   * flag specifies whether the words are chosen deterministically.
   */
  @PolarisImmutable
  abstract class RealisticKeySet implements KeySet {
    @Value.Default
    public int namespaceLevels() {
      return 1;
    }

    @Value.Default
    public int foldersPerLevel() {
      return 5;
    }

    @Value.Default
    public int tablesPerNamespace() {
      return 20;
    }

    @Value.Default
    public boolean deterministic() {
      return true;
    }

    @Override
    public List<IndexKey> keys() {
      // This is the fastest way to generate a ton of keys, tested using profiling/JMH.
      int namespacesFolders = (int) pow(namespaceLevels(), foldersPerLevel());
      Set<IndexKey> namespaces =
          Sets.newHashSetWithExpectedSize(
              namespacesFolders); // actual value is higher, but that's fine here
      Set<IndexKey> keys = new TreeSet<>();

      generateKeys(null, 0, namespaces, keys);

      return new ArrayList<>(keys);
    }

    private void generateKeys(
        IndexKey current, int level, Set<IndexKey> namespaces, Set<IndexKey> keys) {
      if (level > namespaceLevels()) {
        return;
      }

      if (level == namespaceLevels()) {
        // generate tables
        for (int i = 0; i < tablesPerNamespace(); i++) {
          generateTableKey(current, level, keys, i);
        }
        return;
      }

      for (int i = 0; i < foldersPerLevel(); i++) {
        IndexKey folderKey = generateFolderKey(current, level, namespaces, i);
        generateKeys(folderKey, level + 1, namespaces, keys);
      }
    }

    private void generateTableKey(IndexKey current, int level, Set<IndexKey> keys, int i) {
      if (deterministic()) {
        IndexKey tableKey = key(current.toString() + "\u0000" + Words.WORDS.get(i));
        Preconditions.checkArgument(
            keys.add(tableKey), "table - current:%s level:%s i:%s", current, level, i);
      } else {
        while (true) {
          IndexKey tableKey = key(current.toString() + "\u0000" + randomWord());
          if (keys.add(tableKey)) {
            break;
          }
        }
      }
    }

    private IndexKey generateFolderKey(
        IndexKey current, int level, Set<IndexKey> namespaces, int i) {
      if (deterministic()) {
        String folder = Words.WORDS.get(i);
        IndexKey folderKey = current != null ? key(current + "\u0000" + folder) : key(folder);
        Preconditions.checkArgument(
            namespaces.add(folderKey), "namespace - current:%s level:%s i:%s", current, level, i);
        return folderKey;
      } else {
        while (true) {
          String folder = randomWord();
          IndexKey folderKey = current != null ? key(current + "\u0000" + folder) : key(folder);
          if (namespaces.add(folderKey)) {
            return folderKey;
          }
        }
      }
    }
  }

  @PolarisImmutable
  abstract class IndexTestSetGenerator<ELEMENT> {

    public abstract Function<IndexKey, IndexElement<ELEMENT>> elementSupplier();

    public abstract IndexValueSerializer<ELEMENT> elementSerializer();

    @Value.Default
    public KeySet keySet() {
      return ImmutableRealisticKeySet.builder().build();
    }

    public final KeyIndexTestSet<ELEMENT> generateIndexTestSet() {
      var index = newStoreIndex(elementSerializer());

      var keys = keySet().keys();

      for (var key : keys) {
        index.add(elementSupplier().apply(key));
      }

      var serialized = index.serialize();

      // Re-serialize to have "clean" internal values in KeyIndexImpl
      var keyIndex = deserializeStoreIndex(serialized.duplicate(), elementSerializer());

      return KeyIndexTestSet.of(keys, keyIndex.serialize(), keyIndex, index);
    }
  }

  static String randomWord() {
    return Words.WORDS.get(ThreadLocalRandom.current().nextInt(Words.WORDS.size()));
  }

  default IndexKey randomKey() {
    var k = keys();
    var i = ThreadLocalRandom.current().nextInt(k.size());
    return k.get(i);
  }

  default ByteBuffer serialize() {
    return keyIndex().serialize();
  }

  default IndexSpi<ObjRef> deserialize() {
    return deserializeStoreIndex(serializedSafe(), OBJ_REF_SERIALIZER);
  }

  default IndexElement<ELEMENT> randomGetKey() {
    IndexKey key = randomKey();
    return keyIndex().getElement(key);
  }

  class Words {
    private static final List<String> WORDS = new ArrayList<>();

    static {
      // Word list "generated" via:
      //
      // curl https://raw.githubusercontent.com/sindresorhus/word-list/main/words.txt |
      // while read word; do
      //   [[ ${#word} -gt 10 ]] && echo $word
      // done | gzip > words.gz
      //
      try {
        var words = KeyIndexTestSet.class.getResource("words.gz");
        var conn = Objects.requireNonNull(words, "words.gz resource not found").openConnection();
        try (var br =
            new BufferedReader(
                new InputStreamReader(
                    new GZIPInputStream(conn.getInputStream()), StandardCharsets.UTF_8))) {
          String line;
          while ((line = br.readLine()) != null) {
            WORDS.add(line);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}

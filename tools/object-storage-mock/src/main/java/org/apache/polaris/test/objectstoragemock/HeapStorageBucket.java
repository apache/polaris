/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// CODE_COPIED_TO_POLARIS from Project Nessie 0.106.1
package org.apache.polaris.test.objectstoragemock;

import static java.lang.System.currentTimeMillis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class HeapStorageBucket {
  private final TreeMap<String, MockObject> objects = new TreeMap<>();

  private HeapStorageBucket() {}

  public static HeapStorageBucket newHeapStorageBucket() {
    return new HeapStorageBucket();
  }

  public void clear() {
    objects.clear();
  }

  public Map<String, MockObject> objects() {
    return objects;
  }

  public Bucket bucket() {
    return Bucket.builder()
        .object(this::retriever)
        .updater(this::updater)
        .lister(this::lister)
        .deleter(this::deleter)
        .build();
  }

  private boolean deleter(String oid) {
    synchronized (objects) {
      return objects.remove(oid) != null;
    }
  }

  private Stream<Bucket.ListElement> lister(String prefix, String offset) {
    Collection<String> keys;
    synchronized (objects) {
      keys = new ArrayList<>();
      if (prefix != null) {
        String startAt = offset != null && offset.compareTo(prefix) > 0 ? offset : prefix;
        for (String key : objects.tailMap(startAt, true).keySet()) {
          if (!key.startsWith(prefix)) {
            break;
          }
          keys.add(key);
        }
      } else {
        keys.addAll(objects.keySet());
      }
    }
    return keys.stream()
        .map(
            key ->
                new Bucket.ListElement() {
                  @Override
                  public String key() {
                    return key;
                  }

                  @Override
                  public MockObject object() {
                    synchronized (objects) {
                      return objects.get(key);
                    }
                  }
                });
  }

  private Bucket.ObjectUpdater updater(String key, Bucket.UpdaterMode mode) {
    MockObject expected;
    synchronized (objects) {
      expected = objects.get(key);
      switch (mode) {
        case CREATE_NEW:
          if (expected != null) {
            throw new IllegalStateException("Object '" + key + "' already exists");
          }
          break;
        case UPDATE:
          if (expected == null) {
            throw new IllegalStateException("Object '" + key + "' does not exist");
          }
          break;
        case UPSERT:
          break;
        default:
          throw new IllegalArgumentException(mode.name());
      }
    }
    return new HeapObjectUpdater(expected, key);
  }

  private MockObject retriever(String key) {
    synchronized (objects) {
      return objects.get(key);
    }
  }

  private class HeapObjectUpdater implements Bucket.ObjectUpdater {
    private final MockObject expected;
    private final String key;
    private final ImmutableMockObject.Builder object;

    public HeapObjectUpdater(MockObject expected, String key) {
      this.expected = expected;
      this.key = key;
      this.object = ImmutableMockObject.builder();
      if (expected != null) {
        this.object.from(expected);
      }
    }

    @Override
    public Bucket.ObjectUpdater setContentType(String contentType) {
      object.contentType(contentType);
      return this;
    }

    @Override
    public Bucket.ObjectUpdater flush() {
      return this;
    }

    @Override
    public MockObject commit() {
      synchronized (objects) {
        MockObject curr = objects.get(key);
        if (curr != expected) {
          throw new ConcurrentModificationException(
              "Object '" + key + "' has been modified concurrently.");
        }
        MockObject obj = object.lastModified(currentTimeMillis()).build();
        objects.put(key, obj);
        return obj;
      }
    }

    @Override
    public Bucket.ObjectUpdater append(long position, InputStream data) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try {
        object.build().writer().write(null, out);
        data.transferTo(out);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      byte[] newData = out.toByteArray();

      object
          .contentLength(newData.length)
          // .etag("etag")
          // .storageClass(StorageClass.STANDARD)
          .writer(
              ((range, output) -> {
                if (range == null || range.everything()) {
                  output.write(newData);
                } else {
                  int offset = (int) range.start();
                  long len = Math.min(newData.length - offset, range.length());
                  output.write(newData, offset, (int) len);
                }
              }));

      return this;
    }
  }
}

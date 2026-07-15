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
package org.apache.polaris.service.catalog.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.iceberg.encryption.Ciphers;
import org.apache.iceberg.encryption.KeyManagementClient;

/** Reversible in-memory KMS used by Polaris encryption tests. */
public class PolarisTestKms implements KeyManagementClient {
  public static final String MASTER_KEY_NAME = "keyA";
  private static final byte[] MASTER_KEY = "0123456789012345".getBytes(StandardCharsets.UTF_8);

  private static boolean closed;

  public static void resetClosed() {
    closed = false;
  }

  public static boolean wasClosed() {
    return closed;
  }

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
    return ByteBuffer.wrap(
        new Ciphers.AesGcmEncryptor(masterKey(wrappingKeyId)).encrypt(bytes(key), null));
  }

  @Override
  public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
    return ByteBuffer.wrap(
        new Ciphers.AesGcmDecryptor(masterKey(wrappingKeyId)).decrypt(bytes(wrappedKey), null));
  }

  @Override
  public void initialize(Map<String, String> properties) {}

  @Override
  public void close() {
    closed = true;
  }

  private static byte[] masterKey(String keyId) {
    if (!MASTER_KEY_NAME.equals(keyId)) {
      throw new IllegalArgumentException("Unknown test master key: " + keyId);
    }
    return MASTER_KEY;
  }

  private static byte[] bytes(ByteBuffer buffer) {
    ByteBuffer copy = buffer.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }
}

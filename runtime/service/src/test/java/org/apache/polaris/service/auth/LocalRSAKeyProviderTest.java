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

package org.apache.polaris.service.auth;

import static org.assertj.core.api.InstanceOfAssertFactories.BYTE_ARRAY;

import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(SoftAssertionsExtension.class)
public class LocalRSAKeyProviderTest {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void fromFiles(@TempDir Path tempDir) throws Exception {
    var publicKeyFile = tempDir.resolve("public.key");
    var privateKeyFile = tempDir.resolve("private.key");
    PemUtils.generateKeyPairFiles(privateKeyFile, publicKeyFile);

    var generatedPublicKey = PemUtils.readPublicKeyFromFile(publicKeyFile, "RSA");
    var generatedPrivateKey = PemUtils.readPrivateKeyFromFile(privateKeyFile, "RSA");

    var keyProvider = LocalRSAKeyProvider.fromFiles(publicKeyFile, privateKeyFile);
    soft.assertThat(keyProvider)
        .extracting(KeyProvider::getPublicKey)
        .extracting(PublicKey::getEncoded, BYTE_ARRAY)
        .containsExactly(generatedPublicKey.getEncoded());
    soft.assertThat(keyProvider)
        .extracting(KeyProvider::getPrivateKey)
        .extracting(PrivateKey::getEncoded, BYTE_ARRAY)
        .containsExactly(generatedPrivateKey.getEncoded());
  }

  @Test
  public void onHeap() throws Exception {
    var keyPair = PemUtils.generateKeyPair();

    var generatedPublicKey = keyPair.getPublic();
    var generatedPrivateKey = keyPair.getPrivate();

    var keyProvider = new LocalRSAKeyProvider(keyPair);
    soft.assertThat(keyProvider)
        .extracting(KeyProvider::getPublicKey)
        .extracting(PublicKey::getEncoded, BYTE_ARRAY)
        .containsExactly(generatedPublicKey.getEncoded());
    soft.assertThat(keyProvider)
        .extracting(KeyProvider::getPrivateKey)
        .extracting(PrivateKey::getEncoded, BYTE_ARRAY)
        .containsExactly(generatedPrivateKey.getEncoded());
  }
}

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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

public class PemUtils {

  private static byte[] parsePEMFile(File pemFile) throws IOException {
    if (!pemFile.isFile() || !pemFile.exists()) {
      throw new FileNotFoundException(
          String.format("The file '%s' doesn't exist.", pemFile.getAbsolutePath()));
    }
    PemReader reader = new PemReader(new FileReader(pemFile, UTF_8));
    PemObject pemObject = reader.readPemObject();
    byte[] content = pemObject.getContent();
    reader.close();
    return content;
  }

  private static PublicKey getPublicKey(byte[] keyBytes, String algorithm) {
    PublicKey publicKey = null;
    try {
      KeyFactory kf = KeyFactory.getInstance(algorithm);
      EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
      publicKey = kf.generatePublic(keySpec);
    } catch (NoSuchAlgorithmException e) {
      System.out.println(
          "Could not reconstruct the public key, the given algorithm could not be found.");
    } catch (InvalidKeySpecException e) {
      System.out.println("Could not reconstruct the public key");
    }

    return publicKey;
  }

  private static PrivateKey getPrivateKey(byte[] keyBytes, String algorithm) {
    PrivateKey privateKey = null;
    try {
      KeyFactory kf = KeyFactory.getInstance(algorithm);
      EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
      privateKey = kf.generatePrivate(keySpec);
    } catch (NoSuchAlgorithmException e) {
      System.out.println(
          "Could not reconstruct the private key, the given algorithm could not be found.");
    } catch (InvalidKeySpecException e) {
      System.out.println("Could not reconstruct the private key");
    }

    return privateKey;
  }

  public static PublicKey readPublicKeyFromFile(String filepath, String algorithm)
      throws IOException {
    byte[] bytes = PemUtils.parsePEMFile(new File(filepath));
    return PemUtils.getPublicKey(bytes, algorithm);
  }

  public static PrivateKey readPrivateKeyFromFile(String filepath, String algorithm)
      throws IOException {
    byte[] bytes = PemUtils.parsePEMFile(new File(filepath));
    return PemUtils.getPrivateKey(bytes, algorithm);
  }
}

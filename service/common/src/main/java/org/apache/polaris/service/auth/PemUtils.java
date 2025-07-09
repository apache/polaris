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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public class PemUtils {

  private static byte[] parsePEMFile(Path pemPath) throws IOException {
    if (!Files.isRegularFile(pemPath) || !Files.exists(pemPath)) {
      throw new FileNotFoundException(
          String.format("The file '%s' doesn't exist.", pemPath.toAbsolutePath()));
    }
    try (BufferedReader reader = Files.newBufferedReader(pemPath, UTF_8)) {
      final StringBuilder encodedBuilder = new StringBuilder();

      boolean headerFound = false;
      boolean footerFound = false;

      String line = reader.readLine();
      while (line != null) {
        if (line.startsWith("-----BEGIN")) {
          headerFound = true;
        } else if (line.startsWith("-----END")) {
          footerFound = true;
          // Stop reading after finding footer
          break;
        } else if (!line.isBlank()) {
          encodedBuilder.append(line);
        }

        line = reader.readLine();
      }

      final byte[] parsed;
      if (headerFound) {
        if (footerFound) {
          final String encoded = encodedBuilder.toString();
          parsed = Base64.getMimeDecoder().decode(encoded);
        } else {
          throw new IOException("PEM Footer not found");
        }
      } else {
        throw new IOException("PEM Header not found");
      }

      return parsed;
    }
  }

  private static PublicKey getPublicKey(byte[] keyBytes, String algorithm) {
    try {
      KeyFactory kf = KeyFactory.getInstance(algorithm);
      EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
      return kf.generatePublic(keySpec);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(
          "Could not reconstruct the public key, the given algorithm could not be found", e);
    } catch (InvalidKeySpecException e) {
      throw new RuntimeException("Could not reconstruct the public key", e);
    }
  }

  private static PrivateKey getPrivateKey(byte[] keyBytes, String algorithm) {
    try {
      KeyFactory kf = KeyFactory.getInstance(algorithm);
      EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
      return kf.generatePrivate(keySpec);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(
          "Could not reconstruct the private key, the given algorithm could not be found", e);
    } catch (InvalidKeySpecException e) {
      throw new RuntimeException("Could not reconstruct the private key", e);
    }
  }

  public static PublicKey readPublicKeyFromFile(Path filepath, String algorithm)
      throws IOException {
    byte[] bytes = PemUtils.parsePEMFile(filepath);
    return PemUtils.getPublicKey(bytes, algorithm);
  }

  public static PrivateKey readPrivateKeyFromFile(Path filepath, String algorithm)
      throws IOException {
    byte[] bytes = PemUtils.parsePEMFile(filepath);
    return PemUtils.getPrivateKey(bytes, algorithm);
  }

  public static KeyPair generateKeyPair() throws NoSuchAlgorithmException {
    KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
    kpg.initialize(2048);
    return kpg.generateKeyPair();
  }

  public static void generateKeyPairFiles(Path privateFileLocation, Path publicFileLocation)
      throws NoSuchAlgorithmException, IOException {
    writeKeyPairFiles(generateKeyPair(), privateFileLocation, publicFileLocation);
  }

  public static void writeKeyPairFiles(
      KeyPair keyPair, Path privateFileLocation, Path publicFileLocation) throws IOException {
    try (BufferedWriter writer = Files.newBufferedWriter(privateFileLocation, UTF_8)) {
      writer.write("-----BEGIN PRIVATE KEY-----");
      writer.newLine();
      writer.write(Base64.getMimeEncoder().encodeToString(keyPair.getPrivate().getEncoded()));
      writer.newLine();
      writer.write("-----END PRIVATE KEY-----");
      writer.newLine();
    }
    try (BufferedWriter writer = Files.newBufferedWriter(publicFileLocation, UTF_8)) {
      writer.write("-----BEGIN PUBLIC KEY-----");
      writer.newLine();
      writer.write(Base64.getMimeEncoder().encodeToString(keyPair.getPublic().getEncoded()));
      writer.newLine();
      writer.write("-----END PUBLIC KEY-----");
      writer.newLine();
    }
  }
}

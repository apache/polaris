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
package org.apache.polaris.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FileTokenProviderTest {

  @TempDir Path tempDir;

  @Test
  public void testLoadTokenFromFile() throws IOException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("token.txt");
    String expectedToken = "test-bearer-token-123";
    Files.writeString(tokenFile, expectedToken);

    // Create file token provider
    FileTokenProvider provider = new FileTokenProvider(tokenFile.toString(), Duration.ofMinutes(5));

    // Test token retrieval
    String actualToken = provider.getToken();
    assertEquals(expectedToken, actualToken);

    provider.close();
  }

  @Test
  public void testLoadTokenFromFileWithWhitespace() throws IOException {
    // Create a temporary token file with whitespace
    Path tokenFile = tempDir.resolve("token.txt");
    String tokenWithWhitespace = "  test-bearer-token-456  \n\t";
    String expectedToken = "test-bearer-token-456";
    Files.writeString(tokenFile, tokenWithWhitespace);

    // Create file token provider
    FileTokenProvider provider = new FileTokenProvider(tokenFile.toString(), Duration.ofMinutes(5));

    // Test token retrieval (should trim whitespace)
    String actualToken = provider.getToken();
    assertEquals(expectedToken, actualToken);

    provider.close();
  }

  @Test
  public void testTokenRefresh() throws IOException, InterruptedException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("token.txt");
    String initialToken = "initial-token";
    Files.writeString(tokenFile, initialToken);

    // Create file token provider with short refresh interval
    FileTokenProvider provider =
        new FileTokenProvider(tokenFile.toString(), Duration.ofMillis(100));

    // Test initial token
    String token1 = provider.getToken();
    assertEquals(initialToken, token1);

    // Wait for refresh interval to pass
    Thread.sleep(200);

    // Update the file
    String updatedToken = "updated-token";
    Files.writeString(tokenFile, updatedToken);

    // Test that token is refreshed
    String token2 = provider.getToken();
    assertEquals(updatedToken, token2);

    provider.close();
  }

  @Test
  public void testNonExistentFile() {
    // Create file token provider for non-existent file
    FileTokenProvider provider =
        new FileTokenProvider("/non/existent/file.txt", Duration.ofMinutes(5));

    // Test token retrieval (should return null)
    String token = provider.getToken();
    assertNull(token);

    provider.close();
  }

  @Test
  public void testEmptyFile() throws IOException {
    // Create an empty token file
    Path tokenFile = tempDir.resolve("empty.txt");
    Files.writeString(tokenFile, "");

    // Create file token provider
    FileTokenProvider provider = new FileTokenProvider(tokenFile.toString(), Duration.ofMinutes(5));

    // Test token retrieval (should return null for empty file)
    String token = provider.getToken();
    assertNull(token);

    provider.close();
  }

  @Test
  public void testClosedProvider() throws IOException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("token.txt");
    Files.writeString(tokenFile, "test-token");

    // Create and close file token provider
    FileTokenProvider provider = new FileTokenProvider(tokenFile.toString(), Duration.ofMinutes(5));
    provider.close();

    // Test token retrieval after closing (should return null)
    String token = provider.getToken();
    assertNull(token);
  }
}

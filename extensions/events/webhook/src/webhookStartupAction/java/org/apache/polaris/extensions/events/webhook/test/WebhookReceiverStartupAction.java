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
package org.apache.polaris.extensions.events.webhook.test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupAction;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupContext;

/**
 * Binds an HTTP receiver before the Polaris server starts and records webhook POSTs to a capture
 * directory for {@code WebhookEventListenerIT}.
 */
public class WebhookReceiverStartupAction implements PolarisServerStartupAction {

  private final AtomicInteger sequence = new AtomicInteger();
  private HttpServer server;
  private Path captureDir;

  @Override
  public void start(PolarisServerStartupContext context) throws Exception {
    captureDir = Path.of(context.getParameters().get("capture-dir"));
    Files.createDirectories(captureDir);
    clearDirectory(captureDir);
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/hook", this::handle);
    server.start();
    int port = server.getAddress().getPort();
    context
        .getSystemProperties()
        .put("polaris.event-listener.webhook.endpoint", "http://127.0.0.1:" + port + "/hook");
  }

  private void handle(HttpExchange exchange) throws IOException {
    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    int n = sequence.getAndIncrement();
    write(n + ".eventType", header(exchange, "Polaris-Event"));
    write(n + ".signature", header(exchange, "Polaris-Signature-256"));
    write(n + ".authorization", header(exchange, "Authorization"));
    write(n + ".body", body);
    exchange.sendResponseHeaders(200, -1);
    exchange.close();
  }

  private void write(String name, String value) throws IOException {
    Files.writeString(captureDir.resolve(name), value, StandardCharsets.UTF_8);
  }

  private static String header(HttpExchange exchange, String name) {
    String value = exchange.getRequestHeaders().getFirst(name);
    return value == null ? "" : value;
  }

  private static void clearDirectory(Path dir) throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      files.forEach(
          path -> {
            try {
              Files.deleteIfExists(path);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    }
  }

  @Override
  public void close() {
    if (server != null) {
      server.stop(0);
      server = null;
    }
  }
}

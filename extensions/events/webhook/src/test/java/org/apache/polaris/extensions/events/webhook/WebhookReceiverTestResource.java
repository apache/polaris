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
package org.apache.polaris.extensions.events.webhook;

import com.sun.net.httpserver.HttpServer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Binds a loopback HTTP receiver before {@code @QuarkusTest} starts so {@code
 * polaris.event-listener.webhook.endpoint} points at a live port. Production {@link
 * WebhookEventListener#createTransport()} is built during {@code @PostConstruct}.
 */
public class WebhookReceiverTestResource implements QuarkusTestResourceLifecycleManager {

  public record RecordedRequest(
      String body, String signature, String eventType, String authorization, String contentType) {}

  public static final class Capture {
    private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
    private final AtomicInteger status = new AtomicInteger(200);

    public List<RecordedRequest> requests() {
      return requests;
    }

    public void setStatus(int status) {
      this.status.set(status);
    }

    public void clear() {
      requests.clear();
      status.set(200);
    }
  }

  private HttpServer server;
  private final Capture capture = new Capture();

  @Override
  public Map<String, String> start() {
    try {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext(
          "/hook",
          exchange -> {
            String body =
                new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            capture
                .requests()
                .add(
                    new RecordedRequest(
                        body,
                        exchange
                            .getRequestHeaders()
                            .getFirst(WebhookEventListener.SIGNATURE_HEADER),
                        exchange
                            .getRequestHeaders()
                            .getFirst(WebhookEventListener.EVENT_TYPE_HEADER),
                        exchange.getRequestHeaders().getFirst("Authorization"),
                        exchange.getRequestHeaders().getFirst("Content-Type")));
            exchange.sendResponseHeaders(capture.status.get(), -1);
            exchange.close();
          });
      server.start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    int port = server.getAddress().getPort();
    return Map.of("polaris.event-listener.webhook.endpoint", "http://127.0.0.1:" + port + "/hook");
  }

  @Override
  public void stop() {
    if (server != null) {
      server.stop(0);
      server = null;
    }
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(capture, field -> field.getType().equals(Capture.class));
  }
}

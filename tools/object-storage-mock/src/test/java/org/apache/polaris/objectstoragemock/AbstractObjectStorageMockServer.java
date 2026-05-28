/*
 * Copyright (C) 2022 Dremio
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
package org.apache.polaris.objectstoragemock;

import java.util.function.Consumer;
import org.apache.polaris.objectstoragemock.ObjectStorageMock.MockServer;
import org.junit.jupiter.api.AfterEach;

public class AbstractObjectStorageMockServer {
  protected MockServer serverInstance;

  protected MockServer createServer(Consumer<ImmutableObjectStorageMock.Builder> configure) {
    if (serverInstance == null) {
      ImmutableObjectStorageMock.Builder serverBuilder = ObjectStorageMock.builder();

      configure.accept(serverBuilder);

      serverInstance = serverBuilder.build().start();

      onCreated(serverInstance);

      return serverInstance;
    } else {
      throw new IllegalArgumentException("Server already created");
    }
  }

  protected void onCreated(MockServer serverInstance) {}

  @AfterEach
  public void stopServer() throws Exception {
    if (serverInstance != null) {
      try {
        serverInstance.close();
      } finally {
        serverInstance = null;
      }
    }
  }
}

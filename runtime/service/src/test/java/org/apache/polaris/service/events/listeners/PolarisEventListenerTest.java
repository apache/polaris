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

package org.apache.polaris.service.events.listeners;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.service.events.PolarisEvent;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexReader;
import org.junit.jupiter.api.Test;

/** Test that PolarisEventListener interface is well-formed and consistent. */
class PolarisEventListenerTest {

  @Test
  void testAllMethodsWellFormed() {
    for (Method method : PolarisEventListener.class.getMethods()) {
      assertThat(method.isDefault()).as("Method %s is not default", method.getName()).isTrue();
      assertThat(method.getReturnType())
          .as("Method %s does not return void", method.getName())
          .isEqualTo(void.class);
      assertThat(method.getParameters())
          .as("Method %s does not have exactly 1 parameter", method.getName())
          .hasSize(1);
      Class<?> eventType = method.getParameters()[0].getType();
      assertThat(PolarisEvent.class).isAssignableFrom(eventType);
      String expectedMethodName =
          "on" + method.getParameterTypes()[0].getSimpleName().replace("Event", "");
      assertThat(method.getName()).isEqualTo(expectedMethodName);
    }
  }

  @Test
  void testAllEventTypesHandled() throws IOException {
    Index index;
    try (InputStream is = getClass().getClassLoader().getResourceAsStream("META-INF/jandex.idx")) {
      index = new IndexReader(is).read();
    }

    // Find all classes that implement PolarisEvent
    Set<String> eventTypes =
        index.getAllKnownImplementors(DotName.createSimple(PolarisEvent.class)).stream()
            .map(ClassInfo::name)
            .map(DotName::toString)
            .collect(Collectors.toSet());

    assertThat(eventTypes).isNotEmpty();

    // Find all event types that have corresponding listener methods
    Set<String> handledEventTypes =
        Arrays.stream(PolarisEventListener.class.getMethods())
            .filter(method -> method.getParameterCount() == 1)
            .filter(method -> PolarisEvent.class.isAssignableFrom(method.getParameterTypes()[0]))
            .map(method -> method.getParameterTypes()[0].getName())
            .collect(Collectors.toSet());

    // Check that all PolarisEvent implementations have corresponding listener methods
    assertThat(handledEventTypes).containsAll(eventTypes);
  }
}

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

package org.apache.polaris.service.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.ForbiddenException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

public class PolarisEventInterceptorManagerTest {

  @Test
  void returnsOriginalEventWhenNoInterceptorsConfigured() {
    PolarisEventInterceptorManager manager = new PolarisEventInterceptorManager(List.of());

    PolarisEvent originalEvent = dropGenericTableEvent("orders");
    PolarisEvent interceptedEvent = manager.intercept(originalEvent);

    assertThat(interceptedEvent).isSameAs(originalEvent);
  }

  @Test
  void appliesModifyAndUsesModifiedEventForFollowingInterceptors() {
    PolarisEventInterceptor modifyInterceptor =
        event ->
            PolarisEventInterceptor.Result.modify(
                new PolarisEvent(
                    event.type(),
                    event.metadata(),
                    new EventAttributeMap(event.attributes())
                        .put(EventAttributes.GENERIC_TABLE_NAME, "orders__protected")));

    String[] seenBySecondInterceptor = new String[1];
    PolarisEventInterceptor observingInterceptor =
        event -> {
          seenBySecondInterceptor[0] =
              event.attributes().getRequired(EventAttributes.GENERIC_TABLE_NAME);
          return PolarisEventInterceptor.Result.allow();
        };

    PolarisEventInterceptorManager manager =
        new PolarisEventInterceptorManager(List.of(modifyInterceptor, observingInterceptor));

    PolarisEvent interceptedEvent = manager.intercept(dropGenericTableEvent("orders"));

    assertThat(interceptedEvent.attributes().getRequired(EventAttributes.GENERIC_TABLE_NAME))
        .isEqualTo("orders__protected");
    assertThat(seenBySecondInterceptor[0]).isEqualTo("orders__protected");
  }

  @Test
  void throwsForbiddenAndShortCircuitsOnDeny() {
    AtomicBoolean secondInterceptorCalled = new AtomicBoolean(false);

    PolarisEventInterceptor denyInterceptor =
        event -> PolarisEventInterceptor.Result.deny("drop blocked by policy");
    PolarisEventInterceptor secondInterceptor =
        event -> {
          secondInterceptorCalled.set(true);
          return PolarisEventInterceptor.Result.allow();
        };

    PolarisEventInterceptorManager manager =
        new PolarisEventInterceptorManager(List.of(denyInterceptor, secondInterceptor));

    assertThatThrownBy(() -> manager.intercept(dropGenericTableEvent("orders__protected")))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("drop blocked by policy");
    assertThat(secondInterceptorCalled).isFalse();
  }

  @Test
  void throwsWhenInterceptorReturnsNullResult() {
    PolarisEventInterceptor invalidInterceptor = event -> null;

    PolarisEventInterceptorManager manager =
        new PolarisEventInterceptorManager(List.of(invalidInterceptor));

    assertThatThrownBy(() -> manager.intercept(dropGenericTableEvent("orders")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("returned null result");
  }

  @Test
  void propagatesUnexpectedInterceptorExceptions() {
    PolarisEventInterceptor failingInterceptor =
        event -> {
          throw new IllegalStateException("interceptor failure");
        };

    PolarisEventInterceptorManager manager =
        new PolarisEventInterceptorManager(List.of(failingInterceptor));

    assertThatThrownBy(() -> manager.intercept(dropGenericTableEvent("orders")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("interceptor failure");
  }

  private static PolarisEvent dropGenericTableEvent(String tableName) {
    return new PolarisEvent(
        PolarisEventType.BEFORE_DROP_GENERIC_TABLE,
        null,
        new EventAttributeMap().put(EventAttributes.GENERIC_TABLE_NAME, tableName));
  }
}

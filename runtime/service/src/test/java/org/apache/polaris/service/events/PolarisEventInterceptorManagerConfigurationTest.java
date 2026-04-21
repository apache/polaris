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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PolarisEventInterceptorManagerConfigurationTest.Profile.class)
public class PolarisEventInterceptorManagerConfigurationTest {

  @ApplicationScoped
  @Identifier("test-modify")
  public static class TestModifyInterceptor implements PolarisEventInterceptor {
    @Override
    public Result intercept(PolarisEvent event) {
      if (event.type() != PolarisEventType.BEFORE_DROP_GENERIC_TABLE) {
        return Result.allow();
      }

      return Result.modify(
          new PolarisEvent(
              event.type(),
              event.metadata(),
              new EventAttributeMap(event.attributes())
                  .put(EventAttributes.GENERIC_TABLE_NAME, "orders__protected")));
    }
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.event-interceptor.types", "test-modify")
          .build();
    }
  }

  @Inject PolarisEventInterceptorManager manager;

  @Test
  void usesConfiguredInterceptorTypesInOrder() {
    PolarisEvent event =
        new PolarisEvent(
            PolarisEventType.BEFORE_DROP_GENERIC_TABLE,
            null,
            new EventAttributeMap().put(EventAttributes.GENERIC_TABLE_NAME, "orders"));

    PolarisEvent intercepted = manager.intercept(event);

    assertThat(intercepted.attributes().getRequired(EventAttributes.GENERIC_TABLE_NAME))
        .isEqualTo("orders__protected");
  }
}

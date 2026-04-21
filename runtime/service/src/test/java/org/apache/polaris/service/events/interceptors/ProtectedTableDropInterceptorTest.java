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

package org.apache.polaris.service.events.interceptors;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventInterceptor;
import org.apache.polaris.service.events.PolarisEventType;
import org.junit.jupiter.api.Test;

public class ProtectedTableDropInterceptorTest {

  @Test
  void deniesGenericTableDropWhenProtectedTagExists() {
    ProtectedTableDropInterceptor interceptor =
        new ProtectedTableDropInterceptor(configuration("protected", "__"));

    PolarisEvent event =
        new PolarisEvent(
            PolarisEventType.BEFORE_DROP_GENERIC_TABLE,
            null,
            new EventAttributeMap().put(EventAttributes.GENERIC_TABLE_NAME, "orders__protected"));

    PolarisEventInterceptor.Result result = interceptor.intercept(event);

    assertThat(result.action()).isEqualTo(PolarisEventInterceptor.Result.Action.DENY);
    assertThat(result.reason()).contains("orders__protected");
  }

  @Test
  void deniesIcebergTableDropWhenProtectedTagExists() {
    ProtectedTableDropInterceptor interceptor =
        new ProtectedTableDropInterceptor(configuration("protected", "__"));

    PolarisEvent event =
        new PolarisEvent(
            PolarisEventType.BEFORE_DROP_TABLE,
            null,
            new EventAttributeMap().put(EventAttributes.TABLE_NAME, "orders__protected"));

    PolarisEventInterceptor.Result result = interceptor.intercept(event);

    assertThat(result.action()).isEqualTo(PolarisEventInterceptor.Result.Action.DENY);
    assertThat(result.reason()).contains("orders__protected");
  }

  @Test
  void allowsDropWhenProtectedTagDoesNotExist() {
    ProtectedTableDropInterceptor interceptor =
        new ProtectedTableDropInterceptor(configuration("protected", "__"));

    PolarisEvent event =
        new PolarisEvent(
            PolarisEventType.BEFORE_DROP_GENERIC_TABLE,
            null,
            new EventAttributeMap().put(EventAttributes.GENERIC_TABLE_NAME, "orders"));

    PolarisEventInterceptor.Result result = interceptor.intercept(event);

    assertThat(result.action()).isEqualTo(PolarisEventInterceptor.Result.Action.ALLOW);
  }

  @Test
  void ignoresNonDropEvents() {
    ProtectedTableDropInterceptor interceptor =
        new ProtectedTableDropInterceptor(configuration("protected", "__"));

    PolarisEvent event =
        new PolarisEvent(
            PolarisEventType.BEFORE_LOAD_GENERIC_TABLE,
            null,
            new EventAttributeMap().put(EventAttributes.GENERIC_TABLE_NAME, "orders__protected"));

    PolarisEventInterceptor.Result result = interceptor.intercept(event);

    assertThat(result.action()).isEqualTo(PolarisEventInterceptor.Result.Action.ALLOW);
  }

  private static ProtectedTableDropInterceptorConfiguration configuration(
      String tagToken, String separator) {
    return new ProtectedTableDropInterceptorConfiguration() {
      @Override
      public String tagToken() {
        return tagToken;
      }

      @Override
      public String separator() {
        return separator;
      }
    };
  }
}

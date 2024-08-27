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
package org.apache.polaris.core.resource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.polaris.core.monitor.PolarisMetricRegistry;

/**
 * Annotation to specify metrics to be registered on initialization. Users need to explicitly call
 * {@link PolarisMetricRegistry#init} to register the metrics.
 *
 * <p>If used on a Jersey resource method, this annotation also serves as a marker for {@code
 * org.apache.polaris.service.TimedApplicationEventListener} to time the underlying method and count
 * errors on failures.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface TimedApi {
  /**
   * The name of the metric to be recorded.
   *
   * @return the metric name
   */
  String value();
}

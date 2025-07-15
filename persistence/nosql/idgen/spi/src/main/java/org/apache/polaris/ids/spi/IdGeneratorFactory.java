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
package org.apache.polaris.ids.spi;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.LongSupplier;
import org.apache.polaris.ids.api.IdGenerator;

public interface IdGeneratorFactory<I extends IdGenerator> {
  String name();

  void validateParameters(Map<String, String> params, IdGeneratorSource idGeneratorSource);

  I buildIdGenerator(Map<String, String> params, IdGeneratorSource idGeneratorSource);

  I buildSystemIdGenerator(Map<String, String> params, LongSupplier clockMillis);

  static IdGeneratorFactory<?> lookupFactory(String name) {
    for (IdGeneratorFactory<?> factory : ServiceLoader.load(IdGeneratorFactory.class)) {
      if (factory.name().equals(name)) {
        return factory;
      }
    }
    throw new IllegalArgumentException("No IdGeneratorFactory found for name " + name);
  }
}

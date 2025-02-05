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
package org.apache.polaris.core.policy;

import java.io.InputStream;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

public class BasePolicyValidator implements PolicyValidator {
  @Override
  public boolean validate(Policy policy) {
    try {
      InputStream schemaStream =
          getResourceAsStream("schemas/policies/system/data-compaction/2025-02-03.json");
      JSONObject rawSchema = new JSONObject(new JSONTokener(schemaStream));
      Schema schema = SchemaLoader.load(rawSchema);

      var jsonData = new JSONObject(new JSONTokener(policy.content));

      schema.validate(jsonData);
      return true;
    } catch (ValidationException e) {
      // The JSON failed validation; handle error information
      System.out.println("JSON validation failed: " + e.getMessage());
      // You can also inspect e.getCausingExceptions() for detail on multiple errors
      return false;
    }
  }

  private static InputStream getResourceAsStream(String fileName) {
    return Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
  }
}

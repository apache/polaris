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
package org.apache.polaris.spark.rest;

import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * RESTResponse definition for LoadGenericTable which extends the iceberg RESTResponse. This is
 * currently required because the Iceberg HTTPClient requires the request and response to be a class
 * of RESTRequest and RESTResponse.
 */
public class LoadGenericTableRESTResponse extends LoadGenericTableResponse implements RESTResponse {

  @JsonCreator
  public LoadGenericTableRESTResponse(
      @JsonProperty(value = "table", required = true) GenericTable table) {
    super(table);
  }

  @Override
  public void validate() {}
}

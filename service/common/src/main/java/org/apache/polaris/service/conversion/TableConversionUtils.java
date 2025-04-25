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
package org.apache.polaris.service.conversion;

import java.util.HashMap;
import org.apache.iceberg.Table;
import org.apache.polaris.service.types.GenericTable;

/** A collection of utility member and methods related to table conversion. */
public class TableConversionUtils {

  public static String FORMAT_ICEBERG = "iceberg";
  public static String PROPERTY_LOCATION = "location";

  public static GenericTable convertToGenericTable(Table icebergTable) {
    HashMap<String, String> properties = new HashMap<>(icebergTable.properties());
    properties.put(PROPERTY_LOCATION, icebergTable.location());

    return new GenericTable(
        icebergTable.name(), FORMAT_ICEBERG, "Iceberg table " + icebergTable.name(), properties);
  }
}

/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.core;

public class PolarisConfiguration {

  public static final String ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING =
      "ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING";
  public static final String ALLOW_TABLE_LOCATION_OVERLAP = "ALLOW_TABLE_LOCATION_OVERLAP";
  public static final String ALLOW_NAMESPACE_LOCATION_OVERLAP = "ALLOW_NAMESPACE_LOCATION_OVERLAP";
  public static final String ALLOW_EXTERNAL_METADATA_FILE_LOCATION =
      "ALLOW_EXTERNAL_METADATA_FILE_LOCATION";
  public static final String ENFORCE_GLOBALLY_UNIQUE_TABLE_LOCATIONS =
      "ENFORCE_GLOBALLY_UNIQUE_TABLE_LOCATIONS";
  public static final String ENFORCE_TABLE_LOCATIONS_INSIDE_NAMESPACE_LOCATIONS =
      "ENFORCE_TABLE_LOCATIONS_INSIDE_NAMESPACE_LOCATIONS";

  public static final String ALLOW_OVERLAPPING_CATALOG_URLS = "ALLOW_OVERLAPPING_CATALOG_URLS";

  public static final String CATALOG_ALLOW_UNSTRUCTURED_TABLE_LOCATION =
      "allow.unstructured.table.location";
  public static final String CATALOG_ALLOW_EXTERNAL_TABLE_LOCATION =
      "allow.external.table.location";

  /*
   * Default values for the configuration properties
   */

  public static final boolean DEFAULT_ALLOW_OVERLAPPING_CATALOG_URLS = false;
  public static final boolean DEFAULT_ALLOW_TABLE_LOCATION_OVERLAP = false;
  public static final boolean DEFAULT_ALLOW_EXTERNAL_METADATA_FILE_LOCATION = false;
  public static final boolean DEFAULT_ALLOW_NAMESPACE_LOCATION_OVERLAP = false;
  public static final boolean DEFAULT_ENFORCE_GLOBALLY_UNIQUE_TABLE_LOCATIONS = false;
  public static final boolean DEFAULT_ENFORCE_TABLE_LOCATIONS_INSIDE_NAMESPACE_LOCATIONS = true;

  private PolarisConfiguration() {}
}

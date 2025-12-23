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
package org.apache.polaris.core.persistence.dao.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.entity.PolarisBaseEntity;

/** the return the result of a create-catalog method */
public class CreateCatalogResult extends BaseResult {

  // the catalog which has been created
  private final PolarisBaseEntity catalog;

  // its associated catalog admin role
  private final PolarisBaseEntity catalogAdminRole;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public CreateCatalogResult(@Nonnull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.catalog = null;
    this.catalogAdminRole = null;
  }

  /**
   * Constructor for success
   *
   * @param catalog the catalog
   * @param catalogAdminRole and associated admin role
   */
  public CreateCatalogResult(
      @Nonnull PolarisBaseEntity catalog, @Nonnull PolarisBaseEntity catalogAdminRole) {
    super(ReturnStatus.SUCCESS);
    this.catalog = catalog;
    this.catalogAdminRole = catalogAdminRole;
  }

  @JsonCreator
  private CreateCatalogResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") @Nullable String extraInformation,
      @JsonProperty("catalog") @Nonnull PolarisBaseEntity catalog,
      @JsonProperty("catalogAdminRole") @Nonnull PolarisBaseEntity catalogAdminRole) {
    super(returnStatus, extraInformation);
    this.catalog = catalog;
    this.catalogAdminRole = catalogAdminRole;
  }

  public PolarisBaseEntity getCatalog() {
    return catalog;
  }

  public PolarisBaseEntity getCatalogAdminRole() {
    return catalogAdminRole;
  }
}

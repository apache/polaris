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
package org.apache.polaris.persistence.nosql.coretypes.catalog;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;

@PolarisImmutable
@JsonSerialize(as = ImmutableLocationsObj.class)
@JsonDeserialize(as = ImmutableLocationsObj.class)
public interface LocationsObj extends BaseCommitObj {

  String LOCATIONS_REF_NAME = "locations";

  ObjType TYPE = new LocationsObjType();

  /**
   * Mapping of catalog entity (base) locations.
   *
   * <p>Overlaps can be checked via this index by looking up the desired (base) location and then
   * "walking" forwards and backwards and checking for overlaps on the contained locations (index
   * keys).
   *
   * <p>TODO identify whether and which value is needed. Table-UUID? Generated "stable ID"?
   *
   * <p>TODO "allocate" a location for staged creates (which do NOT create the table in the catalog)
   */
  IndexContainer<ObjRef> locations();

  @Override
  default ObjType type() {
    return TYPE;
  }

  static ImmutableLocationsObj.Builder builder() {
    return ImmutableLocationsObj.builder();
  }

  final class LocationsObjType extends AbstractObjType<LocationsObj> {
    public LocationsObjType() {
      super("locs", "Storage Locations", LocationsObj.class);
    }
  }

  interface Builder extends BaseCommitObj.Builder<LocationsObj, Builder> {}
}

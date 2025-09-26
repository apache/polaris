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
package org.apache.polaris.persistence.nosql.coretypes.principals;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.immutables.value.Value;

@PolarisImmutable
@JsonSerialize(as = ImmutablePrincipalObj.class)
@JsonDeserialize(as = ImmutablePrincipalObj.class)
public interface PrincipalObj extends ObjBase {
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> clientId();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> mainSecretHash();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> secondarySecretHash();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> secretSalt();

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  @Value.Default
  default boolean credentialRotationRequired() {
    return false;
  }

  ObjType TYPE = new PrincipalObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  static Builder builder() {
    return ImmutablePrincipalObj.builder();
  }

  final class PrincipalObjType extends AbstractObjType<PrincipalObj> {
    public PrincipalObjType() {
      super("pr", "Principal", PrincipalObj.class);
    }
  }

  interface Builder extends ObjBase.Builder<PrincipalObj, Builder> {

    @CanIgnoreReturnValue
    Builder from(PrincipalObj from);

    @CanIgnoreReturnValue
    Builder mainSecretHash(String mainSecretHash);

    @CanIgnoreReturnValue
    Builder mainSecretHash(Optional<String> mainSecretHash);

    @CanIgnoreReturnValue
    Builder secondarySecretHash(String secondarySecretHash);

    @CanIgnoreReturnValue
    Builder secondarySecretHash(Optional<String> secondarySecretHash);

    @CanIgnoreReturnValue
    Builder secretSalt(String secretSalt);

    @CanIgnoreReturnValue
    Builder secretSalt(Optional<String> secretSalt);

    @CanIgnoreReturnValue
    Builder clientId(String clientId);

    @CanIgnoreReturnValue
    Builder clientId(Optional<String> clientId);

    @CanIgnoreReturnValue
    Builder credentialRotationRequired(boolean credentialRotationRequired);
  }
}

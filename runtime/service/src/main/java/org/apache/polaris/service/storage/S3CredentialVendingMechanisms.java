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
package org.apache.polaris.service.storage;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;

/**
 * Every {@link S3CredentialVendingMechanism} bean installed in this server, discovered through CDI
 * by its {@code @Identifier} qualifier. A realm's allowlist is checked separately; this registry
 * answers only what is actually installed.
 */
@ApplicationScoped
public class S3CredentialVendingMechanisms {

  private final Function<String, Optional<S3CredentialVendingMechanism>> select;
  private final Set<String> ids;

  @Inject
  public S3CredentialVendingMechanisms(@Any Instance<S3CredentialVendingMechanism> mechanisms) {
    Set<String> found = new TreeSet<>();
    for (Instance.Handle<S3CredentialVendingMechanism> handle : mechanisms.handles()) {
      String id =
          handle.getBean().getQualifiers().stream()
              .filter(Identifier.class::isInstance)
              .map(q -> ((Identifier) q).value())
              .findFirst()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "S3 credential vending mechanism bean "
                              + handle.getBean().getBeanClass().getName()
                              + " has no @Identifier"));
      if (!found.add(id)) {
        throw new IllegalStateException(
            "Two S3 credential vending mechanisms share the identifier " + id);
      }
    }
    this.ids = Set.copyOf(found);
    this.select =
        id -> {
          Instance<S3CredentialVendingMechanism> selected =
              mechanisms.select(Identifier.Literal.of(id));
          return selected.isResolvable() ? Optional.of(selected.get()) : Optional.empty();
        };
  }

  /** For tests: a fixed map of identifier to mechanism. */
  public S3CredentialVendingMechanisms(Map<String, S3CredentialVendingMechanism> mechanisms) {
    this.ids = Set.copyOf(mechanisms.keySet());
    this.select = id -> Optional.ofNullable(mechanisms.get(id));
  }

  public Set<String> availableIds() {
    return ids;
  }

  public boolean isAvailable(String id) {
    return ids.contains(id);
  }

  /** The mechanism for an identifier, or the 400 every gate returns when the server lacks it. */
  public S3CredentialVendingMechanism require(String id) {
    return select
        .apply(id)
        .orElseThrow(
            () ->
                new ValidationException(
                    "S3 credential vending mechanism %s is not available in this server", id));
  }
}

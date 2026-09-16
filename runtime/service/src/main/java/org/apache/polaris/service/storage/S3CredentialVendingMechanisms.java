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

import io.quarkus.arc.ClientProxy;
import io.quarkus.runtime.Startup;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;

/**
 * Every {@link S3CredentialVendingMechanism} installed in this server, resolved once at startup.
 * The identifiers come from the {@code @Identifier} qualifier of every enabled bean, read through
 * the {@link BeanManager} so that an enabled alternative does not hide the bean it replaces; each
 * identifier is then resolved through {@link Instance#select}, which lets the alternative with the
 * highest priority win, and the instance is created so a bean that cannot be built fails startup
 * instead of the first request. A realm's allowlist is checked separately; this registry answers
 * only what is installed.
 */
@ApplicationScoped
@Startup
public class S3CredentialVendingMechanisms {

  private final Map<String, S3CredentialVendingMechanism> mechanisms;

  @Inject
  public S3CredentialVendingMechanisms(
      @Any Instance<S3CredentialVendingMechanism> candidates, BeanManager beanManager) {
    // getBeans(Type, Annotation...) returns every enabled bean without ambiguity resolution.
    // Instance#handles() on the unqualified @Any Instance would resolve ambiguity across all
    // mechanism beans, which drops the bean an enabled alternative replaces (and any bean carrying
    // the DEFAULT identifier once a non-default alternative exists).
    Set<String> ids = new TreeSet<>();
    for (Bean<?> bean :
        beanManager.getBeans(S3CredentialVendingMechanism.class, Any.Literal.INSTANCE)) {
      ids.add(identifierOf(bean));
    }
    Map<String, S3CredentialVendingMechanism> resolved = new TreeMap<>();
    for (String id : ids) {
      Instance<S3CredentialVendingMechanism> selected =
          candidates.select(Identifier.Literal.of(id));
      if (selected.isAmbiguous()) {
        throw new IllegalStateException(
            "Two S3 credential vending mechanisms share the identifier " + id);
      }
      if (selected.isUnsatisfied()) {
        throw new IllegalStateException(
            "S3 credential vending mechanism " + id + " resolves to no enabled bean");
      }
      S3CredentialVendingMechanism mechanism = selected.get();
      // Normal-scoped beans hand out a client proxy; unwrapping it creates the instance now, so a
      // constructor or injection failure aborts startup rather than the first request.
      ClientProxy.unwrap(mechanism);
      resolved.put(id, mechanism);
    }
    this.mechanisms = Collections.unmodifiableMap(resolved);
  }

  /**
   * For tests: a live view of identifier to mechanism. The map is read on every call, so a test can
   * install or remove a mechanism after catalogs were created.
   */
  public S3CredentialVendingMechanisms(Map<String, S3CredentialVendingMechanism> mechanisms) {
    this.mechanisms = mechanisms;
  }

  /** The installed identifiers, sorted. */
  public Set<String> availableIds() {
    return Collections.unmodifiableSet(new TreeSet<>(mechanisms.keySet()));
  }

  public boolean isAvailable(String id) {
    return mechanisms.containsKey(id);
  }

  /** The mechanism for an identifier, or the 400 every gate returns when the server lacks it. */
  public S3CredentialVendingMechanism require(String id) {
    S3CredentialVendingMechanism mechanism = mechanisms.get(id);
    if (mechanism == null) {
      throw new ValidationException(
          "S3 credential vending mechanism %s is not available in this server", id);
    }
    return mechanism;
  }

  private static String identifierOf(Bean<?> bean) {
    return bean.getQualifiers().stream()
        .filter(Identifier.class::isInstance)
        .map(qualifier -> ((Identifier) qualifier).value())
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "S3 credential vending mechanism bean "
                        + bean.getBeanClass().getName()
                        + " has no @Identifier"));
  }
}

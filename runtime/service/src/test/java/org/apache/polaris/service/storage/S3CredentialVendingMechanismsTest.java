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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.junit.jupiter.api.Test;

/**
 * {@link S3CredentialVendingMechanisms} discovers identifiers from bean metadata and resolves each
 * identifier once, at construction. No CDI container runs here: the {@link BeanManager} and the
 * {@link Instance} are Mockito fakes that answer only the calls the registry makes.
 */
class S3CredentialVendingMechanismsTest {

  private static final class StsBean {}

  private static final class DefaultBean {}

  private static final class OverridingDefaultBean {}

  private static final class SecondBean {}

  private final BeanManager beanManager = mock(BeanManager.class);

  @SuppressWarnings("unchecked")
  private final Instance<S3CredentialVendingMechanism> candidates = mock(Instance.class);

  @Test
  void identifiersComeFromEveryBeanAndResolutionPicksTheAlternative() {
    S3CredentialVendingMechanism sts = mock(S3CredentialVendingMechanism.class);
    S3CredentialVendingMechanism override = mock(S3CredentialVendingMechanism.class);
    beans(
        bean(StsBean.class, Identifier.Literal.of("STS")),
        bean(DefaultBean.class, Identifier.Literal.of("DEFAULT")),
        bean(OverridingDefaultBean.class, Identifier.Literal.of("DEFAULT")));
    resolves("STS", sts);
    resolves("DEFAULT", override);

    S3CredentialVendingMechanisms registry =
        new S3CredentialVendingMechanisms(candidates, beanManager);

    assertThat(registry.availableIds()).containsExactly("DEFAULT", "STS");
    assertThat(registry.require("DEFAULT")).isSameAs(override);
    assertThat(registry.require("STS")).isSameAs(sts);
  }

  @Test
  void aBeanWithNoIdentifierAbortsStartup() {
    beans(bean(StsBean.class));

    assertThatThrownBy(() -> new S3CredentialVendingMechanisms(candidates, beanManager))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "S3 credential vending mechanism bean "
                + StsBean.class.getName()
                + " has no @Identifier");
  }

  @Test
  void twoBeansSharingOneIdentifierAbortStartup() {
    beans(
        bean(StsBean.class, Identifier.Literal.of("DUP")),
        bean(SecondBean.class, Identifier.Literal.of("DUP")));
    ambiguous("DUP");

    assertThatThrownBy(() -> new S3CredentialVendingMechanisms(candidates, beanManager))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Two S3 credential vending mechanisms share the identifier DUP");
  }

  @Test
  void anIdentifierThatResolvesToNoBeanAbortsStartup() {
    beans(bean(StsBean.class, Identifier.Literal.of("GHOST")));
    unsatisfied("GHOST");

    assertThatThrownBy(() -> new S3CredentialVendingMechanisms(candidates, beanManager))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("S3 credential vending mechanism GHOST resolves to no enabled bean");
  }

  @Test
  void aBeanThatFailsToConstructAbortsStartup() {
    beans(bean(StsBean.class, Identifier.Literal.of("STS")));
    failsToConstruct("STS", new IllegalStateException("no STS client"));

    assertThatThrownBy(() -> new S3CredentialVendingMechanisms(candidates, beanManager))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("no STS client");
  }

  @Test
  void requireOnAMissingIdentifierIsTheRefusalEveryGateReturns() {
    beans(bean(StsBean.class, Identifier.Literal.of("STS")));
    resolves("STS", mock(S3CredentialVendingMechanism.class));
    S3CredentialVendingMechanisms registry =
        new S3CredentialVendingMechanisms(candidates, beanManager);

    assertThat(registry.isAvailable("NOPE")).isFalse();
    assertThatThrownBy(() -> registry.require("NOPE"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential vending mechanism NOPE is not available in this server");
  }

  @Test
  void theTestMapIsReadOnEveryCall() {
    Map<String, S3CredentialVendingMechanism> live = new HashMap<>();
    live.put("STS", mock(S3CredentialVendingMechanism.class));
    S3CredentialVendingMechanisms registry = new S3CredentialVendingMechanisms(live);
    assertThat(registry.availableIds()).containsExactly("STS");

    live.put("LATER", mock(S3CredentialVendingMechanism.class));
    assertThat(registry.availableIds()).containsExactly("LATER", "STS");
    assertThat(registry.isAvailable("LATER")).isTrue();

    live.remove("STS");
    assertThat(registry.isAvailable("STS")).isFalse();
    assertThatThrownBy(() -> registry.require("STS"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential vending mechanism STS is not available in this server");
  }

  private void beans(Bean<?>... beans) {
    doReturn(Set.of(beans))
        .when(beanManager)
        .getBeans(S3CredentialVendingMechanism.class, Any.Literal.INSTANCE);
  }

  @SuppressWarnings("unchecked")
  private static Bean<?> bean(Class<?> beanClass, Annotation... qualifiers) {
    Bean<S3CredentialVendingMechanism> bean = mock(Bean.class);
    doReturn(beanClass).when(bean).getBeanClass();
    doReturn(Set.of(qualifiers)).when(bean).getQualifiers();
    return bean;
  }

  @SuppressWarnings("unchecked")
  private Instance<S3CredentialVendingMechanism> selection(String id) {
    Instance<S3CredentialVendingMechanism> selected = mock(Instance.class);
    doReturn(selected).when(candidates).select(Identifier.Literal.of(id));
    return selected;
  }

  private void resolves(String id, S3CredentialVendingMechanism mechanism) {
    Instance<S3CredentialVendingMechanism> selected = selection(id);
    doReturn(false).when(selected).isAmbiguous();
    doReturn(false).when(selected).isUnsatisfied();
    doReturn(mechanism).when(selected).get();
  }

  private void ambiguous(String id) {
    Instance<S3CredentialVendingMechanism> selected = selection(id);
    doReturn(true).when(selected).isAmbiguous();
  }

  private void unsatisfied(String id) {
    Instance<S3CredentialVendingMechanism> selected = selection(id);
    doReturn(false).when(selected).isAmbiguous();
    doReturn(true).when(selected).isUnsatisfied();
  }

  private void failsToConstruct(String id, RuntimeException failure) {
    Instance<S3CredentialVendingMechanism> selected = selection(id);
    doReturn(false).when(selected).isAmbiguous();
    doReturn(false).when(selected).isUnsatisfied();
    doThrow(failure).when(selected).get();
  }
}

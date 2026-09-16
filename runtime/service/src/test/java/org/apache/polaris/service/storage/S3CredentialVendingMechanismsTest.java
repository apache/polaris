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
import static org.mockito.Mockito.mock;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.junit.jupiter.api.Test;

/**
 * {@link S3CredentialVendingMechanisms} discovers its beans by reading {@code Instance.handles()}
 * alone; {@code get()} is never called during discovery, so these tests build a fake {@link
 * Instance} whose handles expose only the {@link Bean} metadata the registry reads: the bean class
 * and its qualifiers. No CDI container runs here.
 */
class S3CredentialVendingMechanismsTest {

  private static final class FirstBean {}

  private static final class SecondBean {}

  @Test
  void aBeanWithNoIdentifierAbortsStartup() {
    Instance<S3CredentialVendingMechanism> mechanisms = instanceOf(handleFor(FirstBean.class));

    assertThatThrownBy(() -> new S3CredentialVendingMechanisms(mechanisms))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "S3 credential vending mechanism bean "
                + FirstBean.class.getName()
                + " has no @Identifier");
  }

  @Test
  void twoBeansSharingOneIdentifierAbortStartup() {
    Instance<S3CredentialVendingMechanism> mechanisms =
        instanceOf(
            handleFor(FirstBean.class, Identifier.Literal.of("DUP")),
            handleFor(SecondBean.class, Identifier.Literal.of("DUP")));

    assertThatThrownBy(() -> new S3CredentialVendingMechanisms(mechanisms))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Two S3 credential vending mechanisms share the identifier DUP");
  }

  @Test
  void twoDistinctIdentifiersBothRegister() {
    Instance<S3CredentialVendingMechanism> mechanisms =
        instanceOf(
            handleFor(FirstBean.class, Identifier.Literal.of("ONE")),
            handleFor(SecondBean.class, Identifier.Literal.of("TWO")));

    S3CredentialVendingMechanisms registry = new S3CredentialVendingMechanisms(mechanisms);

    assertThat(registry.availableIds()).containsExactlyInAnyOrder("ONE", "TWO");
  }

  @SafeVarargs
  @SuppressWarnings("unchecked")
  private static Instance<S3CredentialVendingMechanism> instanceOf(
      Instance.Handle<S3CredentialVendingMechanism>... handles) {
    Instance<S3CredentialVendingMechanism> instance = mock(Instance.class);
    doReturn(List.of(handles)).when(instance).handles();
    return instance;
  }

  @SuppressWarnings("unchecked")
  private static Instance.Handle<S3CredentialVendingMechanism> handleFor(
      Class<?> beanClass, Annotation... qualifiers) {
    Bean<S3CredentialVendingMechanism> bean = mock(Bean.class);
    doReturn(beanClass).when(bean).getBeanClass();
    doReturn(Set.of(qualifiers)).when(bean).getQualifiers();
    Instance.Handle<S3CredentialVendingMechanism> handle = mock(Instance.Handle.class);
    doReturn(bean).when(handle).getBean();
    return handle;
  }
}

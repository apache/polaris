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
package org.apache.polaris.service.admin;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.polaris.core.admin.model.Catalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CatalogConfigValidatorsTest {
  @Mock private Instance<CatalogConfigValidator> candidates;
  @Mock private CatalogConfigValidator first;
  @Mock private CatalogConfigValidator second;
  @Mock private Catalog catalog;

  @Test
  void resolvesValidatorsOnceAndReusesThemForEachValidation() {
    when(candidates.stream()).thenReturn(Stream.of(first, second));

    CatalogConfigValidators validators = new CatalogConfigValidators(candidates);
    validators.validate(catalog);
    validators.validate(catalog);

    verify(candidates).stream();
    verifyNoMoreInteractions(candidates);
    verify(first, times(2)).validate(catalog);
    verify(second, times(2)).validate(catalog);
    verifyNoMoreInteractions(first, second);
  }

  @Test
  void acceptsCatalogWhenNoValidatorsAreInstalled() {
    when(candidates.stream()).thenReturn(Stream.empty());

    CatalogConfigValidators validators = new CatalogConfigValidators(candidates);

    assertThatCode(() -> validators.validate(catalog)).doesNotThrowAnyException();
  }

  @Test
  void propagatesDiscoveryFailure() {
    IllegalStateException failure = new IllegalStateException("Cannot resolve validator");
    when(candidates.stream()).thenThrow(failure);

    assertThatThrownBy(() -> new CatalogConfigValidators(candidates)).isSameAs(failure);
  }

  @Test
  void propagatesValidationFailure() {
    IllegalArgumentException failure =
        new IllegalArgumentException("Invalid catalog configuration");
    doThrow(failure).when(first).validate(catalog);
    CatalogConfigValidators validators = new CatalogConfigValidators(List.of(first, second));

    assertThatThrownBy(() -> validators.validate(catalog)).isSameAs(failure);
    verifyNoInteractions(second);
  }

  @Test
  void snapshotsTheProvidedValidatorList() {
    List<CatalogConfigValidator> installed = new ArrayList<>();
    installed.add(first);
    CatalogConfigValidators validators = new CatalogConfigValidators(installed);
    installed.clear();
    installed.add(second);

    validators.validate(catalog);

    verify(first).validate(catalog);
    verifyNoInteractions(second);
  }
}

/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.testing.azurite;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({AzuriteExtension.class, SoftAssertionsExtension.class})
public class ITAzuriteExtensionParameters {
  @InjectSoftAssertions private SoftAssertions soft;

  @Test
  public void fields2(@Azurite AzuriteAccess param1, @Azurite AzuriteAccess param2) {
    soft.assertThat(param1).isNotNull();
    soft.assertThat(param2).isNotNull();
    soft.assertThat(param1).isNotSameAs(param2);

    soft.assertThat(param1.storageContainer()).isNotEmpty();
    soft.assertThat(param2.storageContainer()).isNotEmpty();

    soft.assertThat(param1.storageContainer()).isNotEqualTo(param2.storageContainer());
    soft.assertThat(param1.endpoint()).isNotEqualTo(param2.endpoint());
  }
}

/*
 * Copyright (C) 2024 Dremio
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
package tests.smallrye;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "extremely.nested")
public interface ExtremelyNested extends NestedA, NestedB {
  /** Extremely nested. */
  int extremelyNested();

  @Override
  int nestedA1();

  @Override
  int nestedA2();

  @Override
  int nestedB1();

  @Override
  int nestedA11();

  @Override
  int nestedB12();
}

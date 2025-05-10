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
package tests.properties;

import org.apache.polaris.docs.ConfigDocs.ConfigItem;
import org.apache.polaris.docs.ConfigDocs.ConfigPageGroup;

@ConfigPageGroup(name = "more")
public class MoreProps {
  @ConfigItem public static final String ONE = "one";
  @ConfigItem public static final String TWO = "two";
  @ConfigItem public static final String THREE = "three";

  /**
   * Sum.
   *
   * @hidden hidden thingy
   */
  @ConfigItem public static final String FOUR = "four";

  @ConfigItem @Deprecated public static final String FIVE = "five";
}

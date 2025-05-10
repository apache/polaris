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

@SuppressWarnings("unused")
@ConfigPageGroup(name = "props")
public class ConfigProps {
  /**
   * A property. {@value #VALUE_ONE} is the default value.
   *
   * <p>Some text there.
   */
  @ConfigItem public static final String CONF_ONE = "property.one";

  /**
   * Some summary for checkstyle.
   *
   * <p>Another property, need some words to cause a line break in the value tag here {@value
   * #VALUE_ONE} for testing.
   *
   * <p>Some text there.
   *
   * @deprecated this is deprecated because of {@link #CONF_THREE}.
   */
  @ConfigItem(section = "b b")
  @Deprecated
  public static final String CONF_TWO = "property.two";

  /**
   * Some {@link #VALUE_TWO} more {@link #VALUE_THREE}.
   *
   * <ul>
   *   <li>foo
   *   <li>bar
   *   <li>baz
   * </ul>
   *
   * <p>blah
   *
   * <ul>
   *   <li>FOO
   *   <li>BAR
   *   <li>BAZ
   * </ul>
   *
   * <ul>
   *   <li>foo
   *   <li>bar
   *   <li>baz
   * </ul>
   */
  @ConfigItem(section = "a a a")
  public static final String CONF_THREE = "property.three";

  /** Four. */
  @ConfigItem public static final String CONF_FOUR = "property.four";

  /** Five. */
  @ConfigItem(section = "b b")
  public static final String CONF_FIVE = "property.five";

  public static final String VALUE_ONE = "111";
  public static final String VALUE_TWO = "value two";
  public static final String VALUE_THREE = "one two three";
}

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
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.time.Duration;
import java.util.List;

/**
 * The docs for {@code my.prefix}.
 *
 * <ul>
 *   <li>Some
 *   <li>unordered
 *   <li>list
 * </ul>
 *
 * Some more text.
 *
 * <ol>
 *   <li>one
 *   <li>two
 *   <li>three
 * </ol>
 */
@ConfigMapping(prefix = "my.prefix")
public interface MappedA extends InterfaceOne {
  @WithName("some-weird-name")
  @WithDefault("some-default")
  @Override
  String configOptionFoo();

  @Override
  Duration someDuration();

  OtherMapped nested();

  /**
   * Example &amp; &lt; &gt; &quot; &nbsp; &euro; &reg; &copy;.
   *
   * <ul>
   *   <li><code>
   *       session-iam-statements[0]=&#123;"Effect":"Allow", "Action":"s3:*", "Resource":"arn:aws:s3:::*&#47;alwaysAllowed&#47;*"}
   *       </code>
   *   <li><code>
   *       session-iam-statements[1]=&#123;"Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*&#47;blocked&#47;*"}
   *       </code>
   * </ul>
   */
  List<String> listOfStrings();
}

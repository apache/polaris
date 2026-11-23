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
package org.apache.polaris.docs.generator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestMarkdownFormatter {

  static Stream<Arguments> lineBreaks() {
    return Stream.of(
        Arguments.of("signs\n temporary credentials", "signs temporary credentials"),
        Arguments.of("ends it\n       early.", "ends it early."),
        Arguments.of("foo  \n bar", "foo bar"),
        Arguments.of("foo\n\n\nbar", "foo bar"),
        Arguments.of("foo\t\n\tbar", "foo bar"));
  }

  @ParameterizedTest
  @MethodSource("lineBreaks")
  public void lineBreakBecomesOneSpace(String text, String expected) {
    var target = new MarkdownFormatter.RootTarget();
    target.addText(text);
    assertThat(target.text.toString()).isEqualTo(expected);
  }

  @Test
  public void codeWrappedAcrossLinesIsFollowedDirectlyByPunctuation() {
    var target = new MarkdownFormatter.RootTarget();
    target.addText("for the storage name ");
    target.addCode("\n prod");
    target.addText(". Use");
    assertThat(target.text.toString()).isEqualTo("for the storage name `prod`. Use");
  }

  @Test
  public void codeWrappedAcrossLinesFollowsAnOpeningParenthesisDirectly() {
    var target = new MarkdownFormatter.RootTarget();
    target.addText("credential mode (");
    target.addCode("\n polaris.authentication.credential-mode=EXTERNAL");
    target.addText("), since");
    assertThat(target.text.toString())
        .isEqualTo("credential mode (`polaris.authentication.credential-mode=EXTERNAL`), since");
  }

  @Test
  public void codeWrappedAcrossLinesKeepsOneSpaceBeforeTheNextWord() {
    var target = new MarkdownFormatter.RootTarget();
    target.addText("the realm feature ");
    target.addCode("\n ALLOW_SETTING_S3_ENDPOINTS");
    target.addText(" must stay true");
    assertThat(target.text.toString())
        .isEqualTo("the realm feature `ALLOW_SETTING_S3_ENDPOINTS` must stay true");
  }

  @Test
  public void codeClosedOnTheNextLineIsFollowedDirectlyByPunctuation() {
    var target = new MarkdownFormatter.RootTarget();
    target.addText("set it to ");
    target.addCode("EXTERNAL\n ");
    target.addText(". Use");
    assertThat(target.text.toString()).isEqualTo("set it to `EXTERNAL`. Use");
  }

  @Test
  public void codeEndingInWhitespaceKeepsOneSpaceBeforeTheNextWord() {
    var target = new MarkdownFormatter.RootTarget();
    target.addText("the value ");
    target.addCode("foo ");
    target.addText(" is the default");
    assertThat(target.text.toString()).isEqualTo("the value `foo` is the default");
  }

  @Test
  public void codeOnOneLineKeepsItsPunctuation() {
    var target = new MarkdownFormatter.RootTarget();
    target.addText("and ");
    target.addCode("secret-key");
    target.addText(", and");
    assertThat(target.text.toString()).isEqualTo("and `secret-key`, and");
  }

  @Test
  public void textEndingInWhitespaceGetsOneSeparator() {
    var target = new MarkdownFormatter.RootTarget();
    target.addText("foo ");
    target.addCode("bar");
    assertThat(target.text.toString()).isEqualTo("foo `bar`");
  }
}

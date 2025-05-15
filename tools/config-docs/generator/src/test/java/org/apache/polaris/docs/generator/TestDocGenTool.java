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
package org.apache.polaris.docs.generator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(SoftAssertionsExtension.class)
public class TestDocGenTool {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void docGenTool(@TempDir Path dir) throws Exception {
    var classpath =
        Arrays.stream(System.getProperty("testing.libraries").split(":"))
            .map(Paths::get)
            .collect(Collectors.toList());

    var tool =
        new ReferenceConfigDocsGenerator(List.of(Paths.get("src/test/java")), classpath, dir, true);
    var result = tool.call();
    soft.assertThat(result).isEqualTo(0);

    var fileProps = dir.resolve("props-main.md");
    var filePropsA = dir.resolve("props-a_a_a.md");
    var filePropsB = dir.resolve("props-b_b.md");
    soft.assertThat(fileProps)
        .isRegularFile()
        .content()
        .isEqualTo(
            """
                    | Property | Description |
                    |----------|-------------|
                    | `property.one` | A property. "111" is the default value.   <br><br>Some text there. |
                    | `property.four` | Four.  |
                    """);
    soft.assertThat(filePropsA)
        .isRegularFile()
        .content()
        .isEqualTo(
            """
                    | Property | Description |
                    |----------|-------------|
                    | `property.three` | Some (`value two`) more (`one two three`). <br><br> * foo    <br> * bar    <br> * baz  <br><br>blah   <br><br> * FOO    <br> * BAR    <br> * BAZ  <br><br> * foo    <br> * bar    <br> * baz  <br><br> |
                    """);
    soft.assertThat(filePropsB)
        .isRegularFile()
        .content()
        .isEqualTo(
            """
                    | Property | Description |
                    |----------|-------------|
                    | `property.two` | Some summary for checkstyle. <br><br>Another property, need some words to cause a line break in the value tag here "111" for testing.   <br><br>Some text there.<br><br>_Deprecated_ this is deprecated because of (`property.three`). |
                    | `property.five` | Five.  |
                    """);

    var fileMyPrefix = dir.resolve("smallrye-my_prefix.md");
    var fileMyTypes = dir.resolve("smallrye-my_types.md");
    var fileExtremelyNested = dir.resolve("smallrye-extremely_nested.md");
    var fileVeryNested = dir.resolve("smallrye-very_nested.md");

    soft.assertThat(fileMyPrefix)
        .isRegularFile()
        .content()
        .isEqualTo(
            """
                    The docs for `my.prefix`.\s

                     * Some   \s
                     * unordered   \s
                     * list \s

                    Some more text.  \s

                     1. one   \s
                     1. two   \s
                     1. three

                    | Property | Default Value | Type | Description |
                    |----------|---------------|------|-------------|
                    | `my.prefix.some-weird-name` | `some-default` | `string` | Something that configures something.  |
                    | `my.prefix.some-duration` |  | `duration` | A duration of something.  |
                    | `my.prefix.nested.other-int` |  | `int` |  |
                    | `my.prefix.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |
                    | `my.prefix.list-of-strings` |  | `list of string` | Example & < > "   € ® ©. <br><br> * ` session-iam-statements[0]= {"Effect":"Allow", "Action":"s3:*", "Resource":"arn:aws:s3:::*/alwaysAllowed/*"}        ` <br> * ` session-iam-statements[1]= {"Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*/blocked/*"}        ` <br><br> |
                    | `my.prefix.some-int-thing` |  | `int` | Something int-ish.  |
                    """);
    soft.assertThat(fileMyTypes)
        .isRegularFile()
        .content()
        .isEqualTo(
            """
                    Documentation for `my.types`.

                    | Property | Default Value | Type | Description |
                    |----------|---------------|------|-------------|
                    | `my.types.string` |  | `string` |  |
                    | `my.types.optional-string` |  | `string` |  |
                    | `my.types.duration` |  | `duration` |  |
                    | `my.types.optional-duration` |  | `duration` |  |
                    | `my.types.path` |  | `path` |  |
                    | `my.types.optional-path` |  | `path` |  |
                    | `my.types.uri` |  | `uri` |  |
                    | `my.types.optional-uri` |  | `uri` |  |
                    | `my.types.instant` |  | `instant` |  |
                    | `my.types.optional-instant` |  | `instant` |  |
                    | `my.types.string-list` |  | `list of string` |  |
                    | `my.types.string-string-map.`_`<stringkey>`_ |  | `string` |  |
                    | `my.types.string-duration-map.`_`<key2>`_ |  | `duration` |  |
                    | `my.types.string-path-map.`_`<name>`_ |  | `path` |  |
                    | `my.types.optional-int` |  | `int` |  |
                    | `my.types.optional-long` |  | `long` |  |
                    | `my.types.optional-double` |  | `double` |  |
                    | `my.types.int-boxed` |  | `int` |  |
                    | `my.types.long-boxed` |  | `long` |  |
                    | `my.types.double-boxed` |  | `double` |  |
                    | `my.types.float-boxed` |  | `float` |  |
                    | `my.types.int-prim` |  | `int` |  |
                    | `my.types.long-prim` |  | `long` |  |
                    | `my.types.double-prim` |  | `double` |  |
                    | `my.types.float-prim` |  | `float` |  |
                    | `my.types.bool-boxed` |  | `boolean` |  |
                    | `my.types.bool-prim` |  | `boolean` |  |
                    | `my.types.enum-thing` |  | `ONE, TWO, THREE` |  |
                    | `my.types.optional-enum` |  | `ONE, TWO, THREE` |  |
                    | `my.types.list-of-enum` |  | `list of ONE, TWO, THREE` |  |
                    | `my.types.map-to-enum.`_`<name>`_ |  | `ONE, TWO, THREE` |  |
                    | `my.types.optional-bool` |  | `boolean` |  |
                    | `my.types.mapped-a.some-weird-name` | `some-default` | `string` | Something that configures something.  |
                    | `my.types.mapped-a.some-duration` |  | `duration` | A duration of something.  |
                    | `my.types.mapped-a.nested.other-int` |  | `int` |  |
                    | `my.types.mapped-a.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |
                    | `my.types.mapped-a.list-of-strings` |  | `list of string` | Example & < > "   € ® ©. <br><br> * ` session-iam-statements[0]= {"Effect":"Allow", "Action":"s3:*", "Resource":"arn:aws:s3:::*/alwaysAllowed/*"}        ` <br> * ` session-iam-statements[1]= {"Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*/blocked/*"}        ` <br><br> |
                    | `my.types.mapped-a.some-int-thing` |  | `int` | Something int-ish.  |
                    | `my.types.optional-mapped-a.some-weird-name` | `some-default` | `string` | Something that configures something.  |
                    | `my.types.optional-mapped-a.some-duration` |  | `duration` | A duration of something.  |
                    | `my.types.optional-mapped-a.nested.other-int` |  | `int` |  |
                    | `my.types.optional-mapped-a.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |
                    | `my.types.optional-mapped-a.list-of-strings` |  | `list of string` | Example & < > "   € ® ©. <br><br> * ` session-iam-statements[0]= {"Effect":"Allow", "Action":"s3:*", "Resource":"arn:aws:s3:::*/alwaysAllowed/*"}        ` <br> * ` session-iam-statements[1]= {"Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*/blocked/*"}        ` <br><br> |
                    | `my.types.optional-mapped-a.some-int-thing` |  | `int` | Something int-ish.  |
                    | `my.types.map-string-mapped-a.`_`<mappy>`_`.some-weird-name` | `some-default` | `string` | Something that configures something.  |
                    | `my.types.map-string-mapped-a.`_`<mappy>`_`.some-duration` |  | `duration` | A duration of something.  |
                    | `my.types.map-string-mapped-a.`_`<mappy>`_`.nested.other-int` |  | `int` |  |
                    | `my.types.map-string-mapped-a.`_`<mappy>`_`.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |
                    | `my.types.map-string-mapped-a.`_`<mappy>`_`.list-of-strings` |  | `list of string` | Example & < > "   € ® ©. <br><br> * ` session-iam-statements[0]= {"Effect":"Allow", "Action":"s3:*", "Resource":"arn:aws:s3:::*/alwaysAllowed/*"}        ` <br> * ` session-iam-statements[1]= {"Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*/blocked/*"}        ` <br><br> |
                    | `my.types.map-string-mapped-a.`_`<mappy>`_`.some-int-thing` |  | `int` | Something int-ish.  |
                    | `my.types.some-duration` |  | `duration` | A duration of something.  |
                    | `my.types.config-option-foo` |  | `string` | Something that configures something.  |
                    | `my.types.some-int-thing` |  | `int` | Something int-ish.  |
                    """);
    soft.assertThat(fileExtremelyNested)
        .isRegularFile()
        .content()
        .isEqualTo(
            """
                    | Property | Default Value | Type | Description |
                    |----------|---------------|------|-------------|
                    | `extremely.nested.extremely-nested` |  | `int` | Extremely nested.  |
                    | `extremely.nested.nested-a1` |  | `int` | A1.  |
                    | `extremely.nested.nested-a2` |  | `int` | A2.  |
                    | `extremely.nested.nested-b1` |  | `int` | B1.  |
                    | `extremely.nested.nested-a11` |  | `int` | A11.  |
                    | `extremely.nested.nested-b12` |  | `int` | B12.  |
                    """);
    soft.assertThat(fileVeryNested)
        .isRegularFile()
        .content()
        .isEqualTo(
            """
                    | Property | Default Value | Type | Description |
                    |----------|---------------|------|-------------|
                    | `very.nested.very-nested` |  | `int` | Very nested.  |
                    | `very.nested.nested-a1` |  | `int` | A1.  |
                    | `very.nested.nested-a2` |  | `int` | A2.  |
                    | `very.nested.nested-b1` |  | `int` | B1.  |
                    | `very.nested.nested-a11` |  | `int` | A11.  |
                    | `very.nested.nested-b12` |  | `int` | B12.  |
                    """);

    var fileSectionA = dir.resolve("smallrye-my_types_Section_A.md");
    soft.assertThat(fileSectionA)
        .isRegularFile()
        .content()
        .isEqualTo(
            """
                    Another map of string to `MappedA`, in its own section.

                    | Property | Default Value | Type | Description |
                    |----------|---------------|------|-------------|
                    | `my.types.map.py.`_`<name>`_`.some-weird-name` | `some-default` | `string` | Something that configures something.  |
                    | `my.types.map.py.`_`<name>`_`.some-duration` |  | `duration` | A duration of something.  |
                    | `my.types.map.py.`_`<name>`_`.nested.other-int` |  | `int` |  |
                    | `my.types.map.py.`_`<name>`_`.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |
                    | `my.types.map.py.`_`<name>`_`.list-of-strings` |  | `list of string` | Example & < > "   € ® ©. <br><br> * ` session-iam-statements[0]= {"Effect":"Allow", "Action":"s3:*", "Resource":"arn:aws:s3:::*/alwaysAllowed/*"}        ` <br> * ` session-iam-statements[1]= {"Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*/blocked/*"}        ` <br><br> |
                    | `my.types.map.py.`_`<name>`_`.some-int-thing` |  | `int` | Something int-ish.  |
                    """);

    // Nested sections
    var fileNestedRoot = dir.resolve("smallrye-nested_root.md");
    soft.assertThat(fileNestedRoot)
        .isRegularFile()
        .content()
        .isEqualTo(
            """
                    Doc for NestedSectionsRoot.

                    | Property | Default Value | Type | Description |
                    |----------|---------------|------|-------------|
                    | `nested.root.nested-c.int-not-in-c` |  | `int` |  |
                    """);

    var fileNestedA = dir.resolve("smallrye-nested_root_section_a.md");
    soft.assertThat(fileNestedA)
        .isRegularFile()
        .content()
        .isEqualTo(
            """
                    | Property | Default Value | Type | Description |
                    |----------|---------------|------|-------------|
                    | `nested.root.nested-a.section-a.string-in-a` |  | `string` |  |
                    """);

    var fileNestedB = dir.resolve("smallrye-nested_root_section_b.md");
    soft.assertThat(fileNestedB)
        .isRegularFile()
        .content()
        .isEqualTo(
            """
                    | Property | Default Value | Type | Description |
                    |----------|---------------|------|-------------|
                    | `nested.root.nested-b.section-b.string-in-b` |  | `string` |  |
                    """);

    var fileNestedC = dir.resolve("smallrye-nested_root_section_c.md");
    soft.assertThat(fileNestedC)
        .isRegularFile()
        .content()
        .isEqualTo(
            """
                    | Property | Default Value | Type | Description |
                    |----------|---------------|------|-------------|
                    | `nested.root.nested-c.string-in-c` |  | `string` |  |
                    | `nested.root.nested-c.int-in-c` |  | `int` |  |
                    """);
  }
}

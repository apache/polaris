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
package org.apache.polaris.service.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests of the parser with raw wire strings, as they appear in the query of a request. */
class ReferencedByChainTest {

  private static final TableIdentifier TARGET = TableIdentifier.of("db", "t");

  private static UriInfo uriInfo(String rawQuery) {
    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getRequestUri())
        .thenReturn(
            URI.create(
                "http://localhost/api/catalog/v1/c/namespaces/db/tables/t"
                    + (rawQuery == null ? "" : "?" + rawQuery)));
    return uriInfo;
  }

  private static List<TableIdentifier> parseValue(String rawValue) {
    return ReferencedByChain.parseRawValue(rawValue);
  }

  // ---- the raw query string ----

  @Test
  void absentParameterMeansNoChain() {
    assertThat(ReferencedByChain.parseRawQuery(null)).isEmpty();
    assertThat(ReferencedByChain.parseRawQuery("")).isEmpty();
    assertThat(ReferencedByChain.parseRawQuery("snapshots=all&other=x")).isEmpty();
  }

  @Test
  void emptyValueMeansNoChain() {
    assertThat(ReferencedByChain.parseRawQuery("referenced-by=")).isEmpty();
    assertThat(ReferencedByChain.parseRawQuery("referenced-by")).isEmpty();
  }

  @Test
  void findsTheParameterAmongOthers() {
    assertThat(
            ReferencedByChain.parseRawQuery(
                "snapshots=all&referenced-by=ns%1Fv1&x=referenced-by%3Da%1Fb&&y"))
        .containsExactly(TableIdentifier.of("ns", "v1"));
  }

  @Test
  void matchesAnEncodedParameterName() {
    assertThat(ReferencedByChain.parseRawQuery("referenced%2Dby=ns%1Fv1"))
        .containsExactly(TableIdentifier.of("ns", "v1"));
  }

  @Test
  void ignoresMalformedEncodingInOtherParameters() {
    assertThat(ReferencedByChain.parseRawQuery("a%zz=1&referenced-by=ns%1Fv1&b=%"))
        .containsExactly(TableIdentifier.of("ns", "v1"));
  }

  @Test
  void rejectsRepeatedParameter() {
    assertThatThrownBy(
            () -> ReferencedByChain.parseRawQuery("referenced-by=a%1Fv&referenced-by=b%1Fw"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("repeated");
  }

  // ---- the raw value ----

  @Test
  void parsesSingleEntry() {
    assertThat(parseValue("ns%1Fv1")).containsExactly(TableIdentifier.of("ns", "v1"));
  }

  @Test
  void acceptsLowerCaseHexInTheSeparator() {
    assertThat(parseValue("ns%1fv1")).containsExactly(TableIdentifier.of("ns", "v1"));
  }

  @Test
  void parsesMultiLevelNamespacesInOrder() {
    assertThat(parseValue("a%1Fb%1Fouter,c%1Finner"))
        .containsExactly(
            TableIdentifier.of(Namespace.of("a", "b"), "outer"), TableIdentifier.of("c", "inner"));
  }

  @Test
  void anEncodedCommaIsPartOfTheName() {
    assertThat(parseValue("ns%1Fa%2Cb,n%2cs%1Fv%2C"))
        .containsExactly(TableIdentifier.of("ns", "a,b"), TableIdentifier.of("n,s", "v,"));
  }

  @Test
  void aLiteralCommaSeparatesEntries() {
    assertThat(parseValue("ns%1Fa,ns%1Fb"))
        .containsExactly(TableIdentifier.of("ns", "a"), TableIdentifier.of("ns", "b"));
  }

  @Test
  void decodesEveryPartExactlyOnce() {
    // %252C is the text %2C, %251F the text %1F: decoding must not be applied twice
    assertThat(parseValue("ns%1Fa%252Cb%251Fc"))
        .containsExactly(TableIdentifier.of("ns", "a%2Cb%1Fc"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "%26|&",
        "%3F|?",
        "%23|#",
        "%25|%",
        "%2B|+",
        "+|+",
        "%20| ",
        "%2F|/",
        "%3D|=",
        "%C3%A9|\u00e9",
        "\u00e9|\u00e9",
        "%F0%9F%99%82|\ud83d\ude42",
        "a+b%20c|a+b c",
      })
  void decodesSpecialCharactersWithPlainPercentDecoding(String rawAndDecoded) {
    String[] pair = rawAndDecoded.split("\\|", 2);
    assertThat(parseValue("n" + pair[0] + "%1F" + "v" + pair[0]))
        .containsExactly(TableIdentifier.of("n" + pair[1], "v" + pair[1]));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "noseparator",
        "%1Fv",
        "ns%1F",
        "a%1F%1Fv",
        "ns%1Fv,",
        ",ns%1Fv",
        "ns%1Fv,,ns%1Fw",
        // malformed percent-encoding
        "ns%1Fv%",
        "ns%1Fv%2",
        "ns%1Fv%zz",
        "ns%1Fv%2g",
        "%ns%1Fv",
        // only ASCII hex digits are accepted (these are fullwidth and Arabic-Indic digits)
        "ns%1Fv%\uff11\uff11",
        "ns%1Fv%\u0661\u0662",
        // malformed UTF-8
        "ns%1Fv%FF",
        "ns%1Fv%C3",
        "ns%1Fv%C3%28",
      })
  void rejectsMalformedChain(String rawValue) {
    assertThatThrownBy(() -> parseValue(rawValue))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("referenced-by");
  }

  // ---- reading the request ----

  @ParameterizedTest
  @NullAndEmptySource
  void absentOrEmptyValueMeansNoChainAndTheUriIsNotRead(String decodedValue) {
    UriInfo uriInfo = mock(UriInfo.class);
    assertThat(ReferencedByChain.parse(decodedValue, uriInfo, TARGET, 10)).isEmpty();
    verifyNoInteractions(uriInfo);
  }

  @Test
  void readsTheChainFromTheRawQuery() {
    UriInfo uriInfo = uriInfo("snapshots=all&referenced-by=a%1Fb%1Fouter%2Cx,c%1Finner");
    assertThat(ReferencedByChain.parse("decoded value is not used", uriInfo, TARGET, 10))
        .containsExactly(
            TableIdentifier.of(Namespace.of("a", "b"), "outer,x"),
            TableIdentifier.of("c", "inner"));
  }

  @Test
  void rejectsAValueTheRawQueryDoesNotHave() {
    assertThatThrownBy(() -> ReferencedByChain.parse("x", uriInfo("snapshots=all"), TARGET, 10))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("could not be read");
    assertThatThrownBy(() -> ReferencedByChain.parse("x", uriInfo(null), TARGET, 10))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("could not be read");
    assertThatThrownBy(() -> ReferencedByChain.parse("x", uriInfo("referenced-by="), TARGET, 10))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("could not be read");
  }

  @Test
  void rejectsAValueWhenTheRequestUriIsNotAvailable() {
    UriInfo noUri = mock(UriInfo.class);
    assertThatThrownBy(() -> ReferencedByChain.parse("x", noUri, TARGET, 10))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("not available");

    UriInfo noRequest = mock(UriInfo.class);
    when(noRequest.getRequestUri()).thenThrow(new IllegalStateException("no request"));
    assertThatThrownBy(() -> ReferencedByChain.parse("x", noRequest, TARGET, 10))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("not available");
  }

  // ---- validation of the chain ----

  @Test
  void acceptsChainAtMaxDepth() {
    assertThat(ReferencedByChain.parse("x", uriInfo("referenced-by=n%1Fa,n%1Fb"), TARGET, 2))
        .hasSize(2);
  }

  @Test
  void rejectsChainDeeperThanMaxDepth() {
    assertThatThrownBy(
            () -> ReferencedByChain.parse("x", uriInfo("referenced-by=n%1Fa,n%1Fb"), TARGET, 1))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("maximum is 1");
  }

  @Test
  void rejectsRepeatedEntry() {
    assertThatThrownBy(() ->
            ReferencedByChain.parse("x", uriInfo("referenced-by=n%1Fa,n%1Fb,n%1Fa"), TARGET, 10))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("repeats");
  }

  @Test
  void rejectsChainContainingTarget() {
    assertThatThrownBy(
            () -> ReferencedByChain.parse("x", uriInfo("referenced-by=n%1Fa,db%1Ft"), TARGET, 10))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("contains the target");
  }

  @Test
  void parseWithoutTargetDoesNotValidateChain() {
    assertThat(ReferencedByChain.parse("x", uriInfo("referenced-by=n%1Fa,n%1Fa"))).hasSize(2);
  }
}

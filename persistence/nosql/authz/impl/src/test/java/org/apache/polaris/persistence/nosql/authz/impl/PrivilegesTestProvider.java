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
package org.apache.polaris.persistence.nosql.authz.impl;

import static org.apache.polaris.persistence.nosql.authz.api.Privilege.AlternativePrivilege.alternativePrivilege;
import static org.apache.polaris.persistence.nosql.authz.api.Privilege.CompositePrivilege.compositePrivilege;
import static org.apache.polaris.persistence.nosql.authz.api.Privilege.InheritablePrivilege.inheritablePrivilege;
import static org.apache.polaris.persistence.nosql.authz.api.Privilege.NonInheritablePrivilege.nonInheritablePrivilege;

import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.authz.api.Privilege;
import org.apache.polaris.persistence.nosql.authz.spi.PrivilegeDefinition;
import org.apache.polaris.persistence.nosql.authz.spi.PrivilegesProvider;

class PrivilegesTestProvider implements PrivilegesProvider {
  @Override
  public String name() {
    return "TEST-ONLY Privileges";
  }

  @Override
  public Stream<PrivilegeDefinition> privilegeDefinitions() {
    var zero = inheritablePrivilege("zero");
    var one = inheritablePrivilege("one");
    var two = inheritablePrivilege("two");
    var three = inheritablePrivilege("three");
    var four = inheritablePrivilege("four");
    var five = inheritablePrivilege("five");
    var six = inheritablePrivilege("six");
    var seven = inheritablePrivilege("seven");
    var eight = inheritablePrivilege("eight");
    var nine = inheritablePrivilege("nine");
    var nonInherit = nonInheritablePrivilege("nonInherit");
    var oneTwoThree = compositePrivilege("oneTwoThree", one, two, three);
    var duplicateOneTwoThree = compositePrivilege("duplicateOneTwoThree", one, two, three);
    var twoThreeFour = compositePrivilege("twoThreeFour", two, three, four);
    var fiveSix = compositePrivilege("fiveSix", five, six);
    var zeroTwo = alternativePrivilege("zeroTwo", zero, two);
    var eightNine = alternativePrivilege("eightNine", eight, nine);
    var individuals =
        Stream.of(
            zero,
            one,
            two,
            three,
            four,
            five,
            six,
            seven,
            eight,
            nine,
            nonInherit,
            oneTwoThree,
            duplicateOneTwoThree,
            twoThreeFour,
            fiveSix,
            zeroTwo,
            eightNine);

    var manyFoo =
        IntStream.range(0, 128)
            .mapToObj(id -> Privilege.InheritablePrivilege.inheritablePrivilege("foo_" + id));

    return Stream.concat(individuals, manyFoo)
        .map(p -> PrivilegeDefinition.builder().privilege(p).build());
  }
}

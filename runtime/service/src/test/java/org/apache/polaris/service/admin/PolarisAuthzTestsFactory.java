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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;

/**
 * A fluent builder for creating authorization test cases in Polaris.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * return authzTestsBuilder("createTableDirectWithWriteDelegation")
 *         .action(
 *             () ->
 *                 newHandler(Set.of(PRINCIPAL_ROLE1))
 *                     .createTableDirectWithWriteDelegation(
 *                         NS2, createDirectWithWriteDelegationRequest, Optional.empty()))
 *         .cleanupAction(() -> newHandler(Set.of(PRINCIPAL_ROLE2)).dropTableWithPurge(newtable))
 *         .shouldPassWith(PolarisPrivilege.TABLE_CREATE, PolarisPrivilege.TABLE_WRITE_DATA)
 *         .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
 *         .createTests();
 * }</pre>
 */
@PolarisImmutable
public abstract class PolarisAuthzTestsFactory {

  protected abstract String operationName();

  protected abstract PolarisAdminService adminService();

  protected abstract Runnable action();

  protected abstract Optional<Runnable> cleanupAction();

  @Value.Default
  protected String principalName() {
    return PolarisAuthzTestBase.PRINCIPAL_NAME;
  }

  @Value.Default
  protected String catalogName() {
    return PolarisAuthzTestBase.CATALOG_NAME;
  }

  @Value.Default
  protected Function<PolarisPrivilege, PrivilegeResult> grantAction() {
    return privilege ->
        adminService()
            .grantPrivilegeOnCatalogToRole(
                catalogName(), PolarisAuthzTestBase.CATALOG_ROLE1, privilege);
  }

  @Value.Default
  protected Function<PolarisPrivilege, PrivilegeResult> revokeAction() {
    return privilege ->
        adminService()
            .revokePrivilegeOnCatalogFromRole(
                catalogName(), PolarisAuthzTestBase.CATALOG_ROLE1, privilege);
  }

  protected abstract List<Set<PolarisPrivilege>> sufficientPrivilegeSets();

  protected abstract List<Set<PolarisPrivilege>> insufficientPrivilegeSets();

  @Value.Check
  protected PolarisAuthzTestsFactory check() {
    if (sufficientPrivilegeSets().isEmpty() && insufficientPrivilegeSets().isEmpty()) {
      throw new IllegalStateException(
          "At least one privilege set must be specified using succeedsWith() or failsWith()");
    }

    EnumSet<PolarisPrivilege> sufficientIndividualPrivileges =
        sufficientPrivilegeSets().stream()
            .filter(set -> set.size() == 1)
            .flatMap(Set::stream)
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(PolarisPrivilege.class)));

    // Automatically create positive tests for all subsuming privileges of each
    // individually-sufficient privilege, since they all must pass as well.
    List<EnumSet<PolarisPrivilege>> missingSufficientPrivilegeSets =
        sufficientIndividualPrivileges.stream()
            .flatMap(p -> PolarisAuthorizerImpl.subsumingPrivilegesOf(p).stream())
            .distinct()
            .sorted(Comparator.comparing(Enum::name))
            .map(EnumSet::of)
            .filter(set -> !sufficientPrivilegeSets().contains(set))
            .toList();
    if (!missingSufficientPrivilegeSets.isEmpty()) {
      Builder builder = builder().from(this);
      missingSufficientPrivilegeSets.forEach(builder::addSufficientPrivilegeSet);
      return builder.build();
    }

    // Automatically create negative tests for each privilege not present *individually*
    // in the positive tests, as single privileges are assumed to be insufficient by default.
    List<EnumSet<PolarisPrivilege>> missingInsufficientPrivilegeSets =
        EnumSet.complementOf(sufficientIndividualPrivileges).stream()
            .sorted(Comparator.comparing(Enum::name))
            .map(EnumSet::of)
            .filter(set -> !insufficientPrivilegeSets().contains(set))
            .toList();
    if (!missingInsufficientPrivilegeSets.isEmpty()) {
      Builder builder = builder().from(this);
      missingInsufficientPrivilegeSets.forEach(builder::addInsufficientPrivilegeSet);
      return builder.build();
    }

    return this;
  }

  /** Builds and returns the stream of dynamic test nodes. */
  public Stream<DynamicNode> createTests() {
    return Stream.of(buildPositiveTests(), buildNegativeTests());
  }

  public static Builder builder() {
    return ImmutablePolarisAuthzTestsFactory.builder();
  }

  public abstract static class Builder {

    @CanIgnoreReturnValue
    public abstract Builder operationName(String operationName);

    /** Sets the action to test. */
    @CanIgnoreReturnValue
    public abstract Builder action(Runnable action);

    /**
     * Sets the cleanup action to run after each test. Cleanup actions only run on positive tests.
     */
    @CanIgnoreReturnValue
    public abstract Builder cleanupAction(Runnable cleanupAction);

    /**
     * Sets the cleanup action to run after each test. Cleanup actions only run on positive tests.
     */
    @CanIgnoreReturnValue
    public abstract Builder cleanupAction(Optional<? extends Runnable> cleanupAction);

    @CanIgnoreReturnValue
    public abstract Builder principalName(String principalName);

    @CanIgnoreReturnValue
    public abstract Builder catalogName(String catalogName);

    @CanIgnoreReturnValue
    public abstract Builder grantAction(Function<PolarisPrivilege, PrivilegeResult> grantAction);

    @CanIgnoreReturnValue
    public abstract Builder revokeAction(Function<PolarisPrivilege, PrivilegeResult> revokeAction);

    /**
     * Adds a set of privileges that should be sufficient for the operation to succeed. Multiple
     * privileges in a single call are treated as a set of privileges that must all be granted
     * together.
     *
     * <p>When calling this method with a single privilege, it's not necessary to explicitly call it
     * again with subsuming privileges, as the test factory will automatically create positive tests
     * for all subsuming privileges.
     */
    @CanIgnoreReturnValue
    public Builder shouldPassWith(PolarisPrivilege... privileges) {
      return addSufficientPrivilegeSet(Set.of(privileges));
    }

    /**
     * Adds a set of privileges that should be insufficient for the operation (it should fail). This
     * is used to verify that certain privileges do NOT grant access to the operation. Multiple
     * privileges in a single call are treated as a set of privileges that must all be granted
     * together.
     *
     * <p>It's not necessary to call this method with a single privilege, as the test factory will
     * automatically create negative tests for each privilege not present *individually* in the
     * positive tests. (But it's still possible to do so, in order to highlight a specific case in
     * the test method.)
     */
    @CanIgnoreReturnValue
    public Builder shouldFailWith(PolarisPrivilege... privileges) {
      return addInsufficientPrivilegeSet(Set.of(privileges));
    }

    /** Special negative test case that should fail with any individual privilege. */
    @CanIgnoreReturnValue
    public Builder shouldFailWithAnyPrivilege() {
      Arrays.stream(PolarisPrivilege.values()).forEach(this::shouldFailWith);
      return this;
    }

    public Stream<DynamicNode> createTests() {
      return build().createTests();
    }

    // not exposed to test classes

    @CanIgnoreReturnValue
    protected abstract Builder from(PolarisAuthzTestsFactory factory);

    @CanIgnoreReturnValue
    protected abstract Builder adminService(PolarisAdminService adminService);

    @CanIgnoreReturnValue
    protected abstract Builder addSufficientPrivilegeSet(Set<PolarisPrivilege> set);

    @CanIgnoreReturnValue
    protected abstract Builder addInsufficientPrivilegeSet(Set<PolarisPrivilege> set);

    protected abstract PolarisAuthzTestsFactory build();
  }

  private DynamicNode buildPositiveTests() {
    return DynamicContainer.dynamicContainer(
        operationName() + " positive tests",
        sufficientPrivilegeSets().stream()
            .map(
                privilegeSet -> {
                  List<DynamicTest> tests = new ArrayList<>();

                  // Success test with sufficient privileges
                  tests.add(
                      DynamicTest.dynamicTest(
                          operationName()
                              + " should succeed with "
                              + formatPrivilegeSet(privilegeSet),
                          () -> {
                            for (PolarisPrivilege privilege : privilegeSet) {
                              assertThat(grantAction().apply(privilege).isSuccess())
                                  .describedAs("Expected success after granting '%s'", privilege)
                                  .isTrue();
                            }

                            assertThatCode(action()::run)
                                .describedAs(
                                    "Expected success with sufficient privileges '%s'",
                                    privilegeSet)
                                .doesNotThrowAnyException();

                            assertThatCode(cleanupAction().orElse(() -> {})::run)
                                .describedAs(
                                    "Cleanup action with sufficient privileges '%s'", privilegeSet)
                                .doesNotThrowAnyException();
                          }));

                  // "Knockout" tests with one privilege revoked, for multi-privilege sets
                  if (privilegeSet.size() > 1) {
                    for (PolarisPrivilege privilege : privilegeSet) {
                      tests.add(
                          DynamicTest.dynamicTest(
                              operationName() + " should fail without " + privilege.name(),
                              () -> {
                                assertThat(revokeAction().apply(privilege).isSuccess())
                                    .describedAs("Expected success after revoking '%s'", privilege)
                                    .isTrue();
                                assertThatThrownBy(
                                        action()::run,
                                        "Expected ForbiddenException after revoking sufficient privilege '%s' from set '%s'",
                                        privilege,
                                        privilegeSet)
                                    .isInstanceOf(ForbiddenException.class)
                                    .hasMessageContaining(principalName())
                                    .hasMessageContaining("is not authorized");
                                assertThat(grantAction().apply(privilege).isSuccess())
                                    .describedAs("Expected success after granting '%s'", privilege)
                                    .isTrue();
                              }));
                    }
                  }

                  // Failure test with all sufficient privileges revoked
                  tests.add(
                      DynamicTest.dynamicTest(
                          operationName() + " should fail with all sufficient privileges revoked",
                          () -> {
                            for (PolarisPrivilege privilege : privilegeSet) {
                              assertThat(revokeAction().apply(privilege).isSuccess())
                                  .describedAs("Expected success after revoking '%s'", privilege)
                                  .isTrue();
                            }
                            assertThatThrownBy(
                                    action()::run,
                                    "Expected ForbiddenException after revoking all sufficient privileges '%s'",
                                    privilegeSet)
                                .isInstanceOf(ForbiddenException.class)
                                .hasMessageContaining(principalName())
                                .hasMessageContaining("is not authorized");
                          }));

                  return DynamicContainer.dynamicContainer(
                      operationName() + " with " + formatPrivilegeSet(privilegeSet), tests);
                }));
  }

  private DynamicNode buildNegativeTests() {
    return DynamicContainer.dynamicContainer(
        operationName() + " negative tests",
        insufficientPrivilegeSets().stream()
            .map(
                privilegeSet ->
                    DynamicTest.dynamicTest(
                        operationName() + " should fail with " + formatPrivilegeSet(privilegeSet),
                        () -> {
                          try {
                            for (PolarisPrivilege privilege : privilegeSet) {
                              assertThat(grantAction().apply(privilege).isSuccess())
                                  .describedAs("Expected success after granting '%s'", privilege)
                                  .isTrue();
                            }

                            assertThatThrownBy(
                                    action()::run,
                                    "Expected ForbiddenException with insufficient privilege set '%s'",
                                    privilegeSet)
                                .isInstanceOf(ForbiddenException.class)
                                .hasMessageContaining(principalName())
                                .hasMessageContaining("is not authorized");

                          } finally {
                            for (PolarisPrivilege privilege : privilegeSet) {
                              assertThat(revokeAction().apply(privilege).isSuccess())
                                  .describedAs("Expected success after revoking '%s'", privilege)
                                  .isTrue();
                            }
                          }
                        })));
  }

  private static String formatPrivilegeSet(Set<PolarisPrivilege> privilegeSet) {
    return privilegeSet.stream().map(Object::toString).collect(Collectors.joining(" + "));
  }
}

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.immutables.PolarisImmutable;
import org.assertj.core.api.Assertions;
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
 * return assertThatOperation("attachPolicyToNamespace")
 *     .withAction(() -> newWrapper(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, request))
 *     .succeedsWith(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.CATALOG_ATTACH_POLICY)
 *     .succeedsWith(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.TABLE_ATTACH_POLICY)
 *     .failsWith(PolarisPrivilege.NAMESPACE_ATTACH_POLICY)
 *     .build();
 * }</pre>
 */
@PolarisImmutable
public abstract class PolarisAuthzTestsFactory {

  protected abstract String operationName();

  protected abstract PolarisAdminService adminService();

  protected abstract Runnable action();

  protected abstract Optional<Runnable> cleanupAction();

  protected abstract Optional<Runnable> setupAction();

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
  protected void check() {
    if (sufficientPrivilegeSets().isEmpty() && insufficientPrivilegeSets().isEmpty()) {
      throw new IllegalStateException(
          "At least one privilege set must be specified using succeedsWith() or failsWith()");
    }
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

    @CanIgnoreReturnValue
    public abstract Builder action(Runnable action);

    @CanIgnoreReturnValue
    public abstract Builder cleanupAction(Runnable cleanupAction);

    @CanIgnoreReturnValue
    public abstract Builder cleanupAction(Optional<? extends Runnable> cleanupAction);

    @CanIgnoreReturnValue
    public abstract Builder setupAction(Runnable setupAction);

    @CanIgnoreReturnValue
    public abstract Builder setupAction(Optional<? extends Runnable> setupAction);

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
     */
    @CanIgnoreReturnValue
    public Builder shouldFailWith(PolarisPrivilege... privileges) {
      return addInsufficientPrivilegeSet(Set.of(privileges));
    }

    /** Special negative test case that should fail with any privilege. */
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
                  tests.add(
                      DynamicTest.dynamicTest(
                          operationName()
                              + " should succeed with "
                              + formatPrivilegeSet(privilegeSet),
                          () -> {
                            if (setupAction().isPresent()) {
                              try {
                                setupAction().get().run();
                              } catch (Throwable t) {
                                Assertions.fail("Running setupAction, got throwable.", t);
                              }
                            }

                            for (PolarisPrivilege privilege : privilegeSet) {
                              assertSuccess(grantAction().apply(privilege));
                            }

                            try {
                              action().run();
                            } catch (Throwable t) {
                              Assertions.fail(
                                  String.format(
                                      "Expected success with sufficientPrivileges '%s', got throwable instead.",
                                      privilegeSet),
                                  t);
                            }

                            if (cleanupAction().isPresent()) {
                              try {
                                cleanupAction().get().run();
                              } catch (Throwable t) {
                                Assertions.fail(
                                    String.format(
                                        "Running cleanupAction with sufficientPrivileges '%s', got throwable.",
                                        privilegeSet),
                                    t);
                              }
                            }
                          }));

                  if (privilegeSet.size() > 1) {
                    for (PolarisPrivilege privilege : privilegeSet) {
                      tests.add(
                          DynamicTest.dynamicTest(
                              operationName() + " should fail without " + privilege.name(),
                              () -> {
                                assertSuccess(revokeAction().apply(privilege));
                                try {
                                  assertThatThrownBy(() -> action().run())
                                      .isInstanceOf(ForbiddenException.class)
                                      .hasMessageContaining(principalName())
                                      .hasMessageContaining("is not authorized");
                                } catch (Throwable t) {
                                  Assertions.fail(
                                      String.format(
                                          "Expected failure after revoking sufficientPrivilege '%s' from set '%s'",
                                          privilege, privilegeSet),
                                      t);
                                }
                                assertSuccess(grantAction().apply(privilege));
                              }));
                    }
                  }
                  tests.add(
                      DynamicTest.dynamicTest(
                          operationName() + " should fail with all sufficient privileges revoked",
                          () -> {
                            for (PolarisPrivilege privilege : privilegeSet) {
                              assertSuccess(revokeAction().apply(privilege));
                            }
                            try {
                              assertThatThrownBy(() -> action().run())
                                  .isInstanceOf(ForbiddenException.class)
                                  .hasMessageContaining(principalName())
                                  .hasMessageContaining("is not authorized");
                            } catch (Throwable t) {
                              Assertions.fail(
                                  String.format(
                                      "Expected failure after revoking all sufficientPrivileges '%s'",
                                      privilegeSet),
                                  t);
                            }
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
                              assertSuccess(grantAction().apply(privilege));
                            }
                            try {
                              assertThatThrownBy(action()::run)
                                  .isInstanceOf(ForbiddenException.class)
                                  .hasMessageContaining(principalName())
                                  .hasMessageContaining("is not authorized");
                            } catch (Throwable t) {
                              Assertions.fail(
                                  String.format(
                                      "Expected failure with insufficient privilege set '%s'",
                                      privilegeSet),
                                  t);
                            }
                          } finally {
                            for (PolarisPrivilege privilege : privilegeSet) {
                              assertSuccess(revokeAction().apply(privilege));
                            }
                          }
                        })));
  }

  private static void assertSuccess(PrivilegeResult result) {
    assertThat(result.isSuccess()).isTrue();
  }

  private static String formatPrivilegeSet(Set<PolarisPrivilege> privilegeSet) {
    return privilegeSet.stream().map(Object::toString).collect(Collectors.joining(" + "));
  }
}

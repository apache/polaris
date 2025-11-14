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
package org.apache.polaris.maintenance.cel;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.relocated.com.google.api.expr.v1alpha1.Decl;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptCreateException;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.cel.tools.ScriptHost;
import org.projectnessie.cel.types.jackson.JacksonRegistry;

/**
 * Provides a CEL script based "retain reference commit object" predicate used for {@code
 * RetainCollector.refRetain*()} functions.
 *
 * <p>The predicate is coded as a <a href="https://github.com/google/cel-spec/">CEL</a> script,
 * using <a href="https://github.com/projectnessie/cel-java/">cel-java</a>.
 *
 * <p>Micro benchmarks prove that the CEL scripts execute pretty fast, definitely fast enough to
 * justify the flexibility of having scripts in the configuration.
 *
 * <p>The scripts have access to the following declared values:
 *
 * <ul>
 *   <li>{@value #VAR_REF} (string) name of the reference
 *   <li>{@value #VAR_COMMITS} (64-bit int) number of the currently processed commit, starting at
 *       {@code 1}
 *   <li>{@value #VAR_AGE_DAYS} (64-bit int) age of currently processed commit in days
 *   <li>{@value #VAR_AGE_HOURS} (64-bit int) age of currently processed commit in hours
 *   <li>{@value #VAR_AGE_MINUTES} (64-bit int) age of currently processed commit in minutes
 * </ul>
 *
 * <p>Scripts <em>must</em> return a {@code boolean} yielding whether the commit shall be retained.
 * Note that maintenance-service implementations can keep the first not-to-be-retained commit.
 *
 * <p>Example scripts
 *
 * <ul>
 *   <li>{@code ageDays < 30 || commits <= 10} retains the reference history with at least 10
 *       commits and commits that are younger than 30 days
 *   <li>{@code true} retains the whole reference history
 *   <li>{@code false} retains the most recent commit
 * </ul>
 *
 * <p>A static cache retains up to 100 compiled CEL scripts, each up to 24 hours after its last use.
 */
public class CelReferenceContinuePredicate<O extends BaseCommitObj> implements Predicate<O> {
  private static final ScriptHost SCRIPT_HOST =
      ScriptHost.newBuilder().registry(JacksonRegistry.newRegistry()).build();

  private static final Cache<String, Script> SCRIPT_CACHE =
      Caffeine.newBuilder()
          .expireAfterAccess(Duration.ofHours(24))
          .maximumSize(100)
          .scheduler(Scheduler.systemScheduler())
          .build();

  private static final String VAR_REF = "ref";
  private static final String VAR_COMMITS = "commits";
  private static final String VAR_AGE_MINUTES = "ageMinutes";
  private static final String VAR_AGE_HOURS = "ageHours";
  private static final String VAR_AGE_DAYS = "ageDays";

  private static final List<Decl> DECLS =
      List.of(
          Decls.newVar(VAR_REF, Decls.String),
          // Decls.Int == 64 bit (aka Java long)
          Decls.newVar(VAR_COMMITS, Decls.Int),
          Decls.newVar(VAR_AGE_MINUTES, Decls.Int),
          Decls.newVar(VAR_AGE_HOURS, Decls.Int),
          Decls.newVar(VAR_AGE_DAYS, Decls.Int));

  static Script createScript(String source) {
    try {
      return SCRIPT_HOST.buildScript(source).withDeclarations(DECLS).build();
    } catch (ScriptCreateException e) {
      throw new RuntimeException(e);
    }
  }

  static Script getScript(String source) {
    return SCRIPT_CACHE.get(source, CelReferenceContinuePredicate::createScript);
  }

  private final String refName;
  private final Function<O, Duration> objAge;
  private final Script script;
  private long numCommit;

  public CelReferenceContinuePredicate(
      @Nonnull String refName, @Nonnull Persistence persistence, @Nonnull String script) {
    this(refName, persistence::objAge, script);
  }

  public CelReferenceContinuePredicate(
      @Nonnull String refName, @Nonnull Function<O, Duration> objAge, @Nonnull String script) {
    this.refName = refName;
    this.objAge = objAge;
    this.script = getScript(script);
  }

  @Override
  public boolean test(O o) {
    var age = objAge.apply(o);
    var args =
        Map.<String, Object>of(
            VAR_REF, refName,
            VAR_COMMITS, ++numCommit,
            VAR_AGE_MINUTES, age.toMinutes(),
            VAR_AGE_HOURS, age.toHours(),
            VAR_AGE_DAYS, age.toDays());
    try {
      return script.execute(Boolean.class, args);
    } catch (ScriptException e) {
      throw new RuntimeException(e);
    }
  }
}

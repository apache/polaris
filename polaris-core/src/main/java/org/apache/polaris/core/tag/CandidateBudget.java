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
package org.apache.polaris.core.tag;

import com.google.common.base.Preconditions;
import org.apache.polaris.core.tag.exceptions.CandidateBudgetExceededException;

/**
 * How many candidate assignment rows one request may still examine.
 *
 * <p>A page size bounds what a read returns; it does not bound what the read looks at, because a
 * value filter can reject any number of rows before it keeps one. This is the bound on the looking.
 * One instance is created per request and handed to every persistence read that request makes, so a
 * later read sees what the earlier ones spent rather than a fresh allowance, and the request as a
 * whole examines at most the number it was given.
 *
 * <p>The store that examines a row is the one that charges for it, through {@link #examine()},
 * before it looks at the row. A store that has rows left and no budget to examine them with does
 * not answer short, because a short answer reads as the end of the range; it refuses, with {@link
 * CandidateBudgetExceededException}, and the caller decides whether it can resume from a candidate
 * it already consumed or has to report the refusal. A store whose reads are bounded by the rows
 * they return charges those rows with {@link #charge(int)} instead.
 *
 * <p>{@link #unbounded()} is the budget of a read that has no page to bound its work with: the
 * callers that read a definition's whole row set to delete or classify it, and a request whose
 * operator has switched the result limit off. It never refuses.
 */
public final class CandidateBudget {

  private static final CandidateBudget UNBOUNDED = new CandidateBudget(Integer.MAX_VALUE);

  private final int limit;
  private int remaining;

  private CandidateBudget(int limit) {
    this.limit = limit;
    this.remaining = limit;
  }

  /**
   * A budget of the given number of candidates. {@link Integer#MAX_VALUE} is the unbounded budget,
   * the same convention the target read's budget uses.
   */
  public static CandidateBudget of(int candidates) {
    Preconditions.checkArgument(candidates >= 0, "a candidate budget cannot be negative");
    return candidates == Integer.MAX_VALUE ? UNBOUNDED : new CandidateBudget(candidates);
  }

  public static CandidateBudget unbounded() {
    return UNBOUNDED;
  }

  public boolean isUnbounded() {
    return this == UNBOUNDED;
  }

  /** The number this budget started with, for a refusal to name. */
  public int limit() {
    return limit;
  }

  public int remaining() {
    return remaining;
  }

  /** Whether the next examination would be refused. Never true of the unbounded budget. */
  public boolean isExhausted() {
    return !isUnbounded() && remaining == 0;
  }

  /**
   * Charges one candidate, before it is examined.
   *
   * @throws CandidateBudgetExceededException when nothing remains: the caller has a candidate it
   *     may not examine, and must not answer as though it had none
   */
  public void examine() {
    if (isUnbounded()) {
      return;
    }
    if (remaining == 0) {
      throw new CandidateBudgetExceededException(limit);
    }
    remaining--;
  }

  /**
   * Charges candidates a store examined and returned together, for a read bounded by the rows it
   * returns rather than the rows it looks at.
   *
   * @throws CandidateBudgetExceededException when the rows exceed what remains: they were read, but
   *     the request may not consume them, and the caller resumes before them or reports the refusal
   */
  public void charge(int candidates) {
    Preconditions.checkArgument(candidates >= 0, "cannot charge a negative number of candidates");
    if (isUnbounded()) {
      return;
    }
    if (candidates > remaining) {
      throw new CandidateBudgetExceededException(limit);
    }
    remaining -= candidates;
  }

  @Override
  public String toString() {
    return isUnbounded()
        ? "CandidateBudget(unbounded)"
        : "CandidateBudget(" + remaining + " of " + limit + ")";
  }
}

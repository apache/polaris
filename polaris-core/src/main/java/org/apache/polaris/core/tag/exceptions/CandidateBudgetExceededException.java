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
package org.apache.polaris.core.tag.exceptions;

import jakarta.ws.rs.core.Response;
import org.apache.polaris.core.exceptions.PolarisException;

/**
 * A read had candidate rows left to examine and no budget to examine them with. It is thrown by the
 * budget itself, from inside the read, before the row is looked at, so that the read never answers
 * short as though its range had ended: the caller either resumes from a candidate it already
 * consumed or reports that the request could not make progress within its budget.
 */
public class CandidateBudgetExceededException extends PolarisException {

  private final int candidateBudget;

  public CandidateBudgetExceededException(int candidateBudget) {
    super(
        String.format(
            "the request examined the %d candidate assignments its work budget allows and rows remain",
            candidateBudget));
    this.candidateBudget = candidateBudget;
  }

  /** The budget the request was given, for the refusal that names it. */
  public int candidateBudget() {
    return candidateBudget;
  }

  @Override
  public int httpStatusCode() {
    return Response.Status.BAD_REQUEST.getStatusCode();
  }
}

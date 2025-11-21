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
package org.apache.polaris.persistence.nosql.api.commit;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.ref.Reference;

@FunctionalInterface
public interface CommitRetryable<REF_OBJ extends BaseCommitObj, RESULT> {

  /**
   * Called from {@linkplain Committer committer} implementations.
   *
   * <p>Implementations call the {@code refObjSupplier} to retrieve the current reference object
   * using the current state of the reference. Long-running attempt implementations that need to
   * have the reference object early should call the supplier again shortly before from this
   * function and attempt to perform the required checks against the latest state of the reference
   * object. This helps in reducing unnecessary retries when the attempt can be safely applied to
   * the latest state of the reference object.
   *
   * <p>Writes must be triggered via the various {@code write*()} functions on {@link
   * CommitterState}, preferable via {@link CommitterState#writeOrReplace(Object, Obj, Class)}. The
   * {@link String} keys are used as symbolic identifiers, implementations are responsible for
   * providing keys that are unique.
   *
   * <p>Reads must happen via the specialized {@link CommitterState#persistence() Persistence}
   * provided by the committer implementation.
   *
   * @param state Communicate {@linkplain Obj objects} to be persisted via {@link CommitterState}
   * @param refObjSupplier supplier returning the {@linkplain Reference#pointer() current object},
   *     if present. Must be invoked.
   * @return Successful attempts return a non-empty {@link Optional} containing the result. An
   *     {@linkplain Optional#empty() empty optional} indicates that a retry should be attempted.
   * @throws CommitException Instances of this class let the whole commit operation abort.
   */
  @Nonnull
  Optional<REF_OBJ> attempt(
      @Nonnull CommitterState<REF_OBJ, RESULT> state,
      @Nonnull Supplier<Optional<REF_OBJ>> refObjSupplier)
      throws CommitException;
}

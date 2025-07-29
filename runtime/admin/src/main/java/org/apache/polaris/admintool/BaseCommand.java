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
package org.apache.polaris.admintool;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

public abstract class BaseCommand implements Callable<Integer> {

  public static final int EXIT_CODE_USAGE = 2;
  public static final int EXIT_CODE_BOOTSTRAP_ERROR = 3;
  public static final int EXIT_CODE_PURGE_ERROR = 4;
  public static final int EXIT_CODE_SCHEMA_ERROR = 5;

  @Inject MetaStoreManagerFactory metaStoreManagerFactory;

  @Spec CommandSpec spec;
}

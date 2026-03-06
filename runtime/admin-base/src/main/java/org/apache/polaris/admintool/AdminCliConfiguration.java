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

import io.quarkus.arc.All;
import io.quarkus.arc.InstanceHandle;
import io.quarkus.picocli.runtime.PicocliCommandLineFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@ApplicationScoped
class AdminCliConfiguration {

  @Inject @All @AdminSubCommand List<InstanceHandle<Callable<Integer>>> subCommands;

  @Produces
  CommandLine customCommandLine(PicocliCommandLineFactory factory) {
    CommandLine commandLine = factory.create();
    subCommands.forEach(h -> commandLine.addSubcommand(h.getBean().getBeanClass()));
    return commandLine;
  }
}

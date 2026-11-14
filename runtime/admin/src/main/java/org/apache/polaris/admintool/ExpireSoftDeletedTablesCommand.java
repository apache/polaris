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

import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.SoftDeletedTableExpiration;
import picocli.CommandLine;

@CommandLine.Command(
    name = "expire-soft-deleted-tables",
    mixinStandardHelpOptions = true,
    description =
        "Permanently delete Iceberg tables whose soft-delete hold has expired. Names stay reserved"
            + " until this command runs.")
public class ExpireSoftDeletedTablesCommand extends BaseMetaStoreCommand {

  @CommandLine.Option(
      names = {"-r", "--realm"},
      paramLabel = "<realm>",
      required = true,
      description = "The name of a realm to scan.")
  List<String> realms;

  @CommandLine.Option(
      names = {"-c", "--catalog"},
      paramLabel = "<catalog>",
      description =
          "Limit expiry to one catalog. When omitted, every INTERNAL catalog in the realm is"
              + " scanned.")
  String catalog;

  @Override
  public Integer call() {
    try {
      boolean anyFailure = false;
      SoftDeletedTableExpiration expiration = new SoftDeletedTableExpiration();
      for (String realm : realms) {
        RealmContext realmContext = () -> realm;
        PolarisMetaStoreManager metaStoreManager =
            metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
        BasePersistence session = metaStoreManagerFactory.getOrCreateSession(realmContext);
        PolarisCallContext callCtx = new PolarisCallContext(realmContext, session);
        SoftDeletedTableExpiration.Summary summary =
            expiration.expireAll(metaStoreManager, callCtx, catalog);
        spec.commandLine()
            .getOut()
            .printf(
                "Realm %s: expired %d, skipped %d, failed %d.%n",
                realm, summary.expired(), summary.skipped(), summary.failed());
        if (summary.failed() > 0) {
          anyFailure = true;
        }
      }
      if (anyFailure) {
        spec.commandLine().getErr().println("Expiry encountered errors during operation.");
        return EXIT_CODE_EXPIRE_ERROR;
      }
      spec.commandLine().getOut().println("Expiry completed successfully.");
      return 0;
    } catch (Exception e) {
      e.printStackTrace(spec.commandLine().getErr());
      spec.commandLine().getErr().println("Expiry encountered errors during operation.");
      return EXIT_CODE_EXPIRE_ERROR;
    }
  }
}

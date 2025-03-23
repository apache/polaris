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
package org.apache.polaris.extension.persistence.impl.eclipselink;

import org.eclipse.persistence.sessions.DatabaseLogin;
import org.eclipse.persistence.sessions.Session;
import org.eclipse.persistence.sessions.SessionCustomizer;

/**
 * This pattern of injecting a SessionCustomizer is taken from the EclipseLink guide documentation:
 *
 * <p>https://eclipse.dev/eclipselink/documentation/4.0/dbws/dbws.html#performing-intermediate-customization
 */
public class PolarisEclipseLinkSessionCustomizer implements SessionCustomizer {
  @Override
  public void customize(Session session) throws Exception {
    DatabaseLogin databaseLogin = (DatabaseLogin) session.getDatasourceLogin();
    databaseLogin.setTransactionIsolation(DatabaseLogin.TRANSACTION_SERIALIZABLE);
  }
}

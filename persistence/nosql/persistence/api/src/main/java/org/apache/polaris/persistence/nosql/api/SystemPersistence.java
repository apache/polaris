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
package org.apache.polaris.persistence.nosql.api;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Inject;
import jakarta.inject.Qualifier;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.persistence.nosql.nodeids.api.NodeManagement;

/**
 * Qualifier for system-level {@link Persistence} instance against the {@linkplain
 * Realms#SYSTEM_REALM_ID system realm} needed for realm management.
 *
 * <p>The qualified {@link Persistence} instance has a functional {@link IdGenerator}, enabled via a
 * valid {@linkplain NodeManagement#lease() node lease}.
 *
 * <p>A system-realm {@link Persistence} instance can be {@link Inject @Inject}ed as an {@link
 * ApplicationScoped @ApplicationScoped} bean using
 *
 * {@snippet :
 * @ApplicationScoped
 * class MyBean {
 *     @Inject @SystemPersistence Persistence systemPersistence; // @highlight
 * }
 * }
 *
 * @see StartupPersistence
 */
@Target({TYPE, METHOD, PARAMETER, FIELD})
@Retention(RUNTIME)
@Documented
@Qualifier
public @interface SystemPersistence {
  @SuppressWarnings("ClassExplicitlyAnnotation")
  final class Literal extends AnnotationLiteral<SystemPersistence> implements SystemPersistence {}
}

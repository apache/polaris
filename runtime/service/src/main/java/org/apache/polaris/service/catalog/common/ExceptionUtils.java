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

package org.apache.polaris.service.catalog.common;

import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.polaris.core.entity.PolarisEntitySubType;

public class ExceptionUtils {
  private ExceptionUtils() {}

  /**
   * Helper function for when a TABLE_LIKE entity is not found, so we want to throw the appropriate
   * exception. Used in Iceberg APIs, so the Iceberg messages cannot be changed.
   *
   * @param subTypes The subtypes of the entity that the exception should report as non-existing
   */
  public static RuntimeException notFoundExceptionForTableLikeEntity(
      TableIdentifier identifier, List<PolarisEntitySubType> subTypes) {

    // In this case, we assume it's a table
    if (subTypes.size() > 1) {
      return new NoSuchTableException("Table does not exist: %s", identifier);
    } else {
      return notFoundExceptionForTableLikeEntity(identifier, subTypes.getFirst());
    }
  }

  public static RuntimeException notFoundExceptionForTableLikeEntity(
      TableIdentifier identifier, PolarisEntitySubType subType) {
    return notFoundExceptionForTableLikeEntity(identifier.toString(), subType);
  }

  public static RuntimeException notFoundExceptionForTableLikeEntity(
      String identifier, PolarisEntitySubType subType) {
    return subType == PolarisEntitySubType.ICEBERG_VIEW
        ? new NoSuchViewException(
            "%s does not exist: %s", entityNameForSubType(subType), identifier)
        : new NoSuchTableException(
            "%s does not exist: %s", entityNameForSubType(subType), identifier);
  }

  public static RuntimeException alreadyExistsExceptionForTableLikeEntity(
      TableIdentifier identifier, PolarisEntitySubType subType) {
    return alreadyExistsExceptionForTableLikeEntity(identifier.toString(), subType);
  }

  public static RuntimeException alreadyExistsExceptionForTableLikeEntity(
      String identifier, PolarisEntitySubType subType) {
    return new AlreadyExistsException(
        "%s already exists: %s", entityNameForSubType(subType), identifier);
  }

  public static RuntimeException alreadyExistsExceptionWithSameNameForTableLikeEntity(
      TableIdentifier identifier, PolarisEntitySubType subType) {
    return new AlreadyExistsException(
        "%s with same name already exists: %s", entityNameForSubType(subType), identifier);
  }

  public static String entityNameForSubType(PolarisEntitySubType subType) {
    if (subType == null) {
      return "Object";
    }
    return switch (subType) {
      case ICEBERG_VIEW -> "View";
      case GENERIC_TABLE -> "Generic table";
      default -> "Table";
    };
  }

  public static NoSuchNamespaceException noSuchNamespaceException(TableIdentifier identifier) {
    return noSuchNamespaceException(identifier.namespace());
  }

  public static NoSuchNamespaceException noSuchNamespaceException(Namespace namespace) {
    // tests assert this
    var ns = namespace.isEmpty() ? "''" : namespace.toString();
    return new NoSuchNamespaceException("Namespace does not exist: %s", ns);
  }
}

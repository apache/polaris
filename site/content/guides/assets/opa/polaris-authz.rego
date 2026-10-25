# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

package polaris.authz

import future.keywords.if
import future.keywords.in

# Deny by default: every operation must be explicitly allowed below.
default allow := false

# Principals with the "service_admin" role can manage catalogs, namespaces and tables.
#
# Operations that manage Polaris's internal privilege system (principals, principal
# roles, catalog roles, grants, policies, ...) are intentionally left out of this list.
# With OPA configured as the authorizer, Polaris's built-in RBAC checks are bypassed
# entirely, so these operations are not handled elsewhere: they simply fall through to
# the "default allow := false" rule above and are always denied.
allow if {
	"service_admin" in input.actor.roles
	input.action in {
		"LIST_CATALOGS",
		"CREATE_CATALOG",
		"GET_CATALOG",
		"UPDATE_CATALOG",
		"DELETE_CATALOG",
		"LIST_NAMESPACES",
		"CREATE_NAMESPACE",
		"LOAD_NAMESPACE_METADATA",
		"UPDATE_NAMESPACE_PROPERTIES",
		"DROP_NAMESPACE",
		"LIST_TABLES",
		"CREATE_TABLE_DIRECT",
		"LOAD_TABLE",
		"LOAD_TABLE_WITH_READ_DELEGATION",
		"LOAD_TABLE_WITH_WRITE_DELEGATION",
		"UPDATE_TABLE",
		"DROP_TABLE_WITHOUT_PURGE",
		"DROP_TABLE_WITH_PURGE",
	}
}

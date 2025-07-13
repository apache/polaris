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

package org.apache.polaris.core.policy;

import static org.apache.polaris.core.policy.PredefinedPolicyTypes.ACCESS_CONTROL;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.policy.content.AccessControlPolicyContent;

public class PolicyUtil {
  private PolicyUtil() {}

  public static String replaceContextVariable(
      String content, PolicyType policyType, AuthenticatedPolarisPrincipal authenticatedPrincipal) {
    if (policyType == ACCESS_CONTROL) {
      try {
        AccessControlPolicyContent policyContent = AccessControlPolicyContent.fromString(content);
        List<Expression> evaluatedRowFilterExpressions = Lists.newArrayList();
        for (Expression rowFilterExpression : policyContent.getRowFilters()) {
          // check if the expression refers to context variable current_principal_role
          if (rowFilterExpression instanceof UnboundPredicate<?>) {
            UnboundPredicate<?> boundPredicate = (UnboundPredicate<?>) rowFilterExpression;
            // check if this references to current_principal
            if (boundPredicate.ref().name().equals("$current_principal_role")) {
              // compare the literal and replace
              String val = (String) boundPredicate.literal().value();
              if (authenticatedPrincipal.getActivatedPrincipalRoleNames().contains(val)) {
                // TODO: see if we can utilize the expression evaluation of iceberg SDK
                Expression result =
                    boundPredicate.op().equals(Expression.Operation.EQ)
                        ? Expressions.alwaysTrue()
                        : Expressions.alwaysFalse();
                evaluatedRowFilterExpressions.add(result);
              } else {
                Expression result =
                    boundPredicate.op().equals(Expression.Operation.NOT_EQ)
                        ? Expressions.alwaysTrue()
                        : Expressions.alwaysFalse();
                evaluatedRowFilterExpressions.add(result);
              }
            } else if (boundPredicate.ref().name().equals("$current_principal")) {
              String val = (String) boundPredicate.literal().value();
              if (authenticatedPrincipal.getName().equals(val)) {
                Expression result =
                    boundPredicate.op().equals(Expression.Operation.EQ)
                        ? Expressions.alwaysTrue()
                        : Expressions.alwaysFalse();
                evaluatedRowFilterExpressions.add(result);
              } else {
                Expression result =
                    boundPredicate.op().equals(Expression.Operation.NOT_EQ)
                        ? Expressions.alwaysTrue()
                        : Expressions.alwaysFalse();
                evaluatedRowFilterExpressions.add(result);
              }
            } else {
              evaluatedRowFilterExpressions.add(rowFilterExpression);
            }
          }
        }

        policyContent.setRowFilters(evaluatedRowFilterExpressions);
        return AccessControlPolicyContent.toString(policyContent);
      } catch (Exception e) {
        return content;
      }
    }
    return content;
  }

  public static boolean filterApplicablePolicy(
      PolicyEntity policyEntity, AuthenticatedPolarisPrincipal authenticatedPrincipal) {
    if (policyEntity.getPolicyType().equals(ACCESS_CONTROL)) {
      AccessControlPolicyContent content =
          AccessControlPolicyContent.fromString(policyEntity.getContent());
      String applicablePrincipal = content.getPrincipalRole();
      return applicablePrincipal == null
          || authenticatedPrincipal.getActivatedPrincipalRoleNames().isEmpty()
          || authenticatedPrincipal
              .getActivatedPrincipalRoleNames()
              .contains(content.getPrincipalRole());
    }

    return true;
  }
}

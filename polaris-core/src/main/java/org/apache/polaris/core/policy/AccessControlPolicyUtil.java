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
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.policy.content.AccessControlPolicyContent;

public class AccessControlPolicyUtil {
  private AccessControlPolicyUtil() {}

  public static String replaceContextVariable(
      String content, PolicyType policyType, AuthenticatedPolarisPrincipal authenticatedPrincipal) {
    if (policyType == ACCESS_CONTROL) {
      try {
        AccessControlPolicyContent policyContent = AccessControlPolicyContent.fromString(content);
        List<Expression> evaluatedRowFilterExpressions = Lists.newArrayList();

        for (Expression rowFilterExpression : policyContent.getRowFilters()) {
          Expression evaluatedExpression =
              ExpressionVisitors.visit(
                  rowFilterExpression,
                  new ContextVariableReplacementVisitor(authenticatedPrincipal));
          evaluatedRowFilterExpressions.add(evaluatedExpression);
        }

        // also nullify the principal role.
        policyContent.setPrincipalRole(null);
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

  /** Expression visitor that replaces context variables with evaluated expressions */
  private static class ContextVariableReplacementVisitor
      extends ExpressionVisitors.ExpressionVisitor<Expression> {
    private final AuthenticatedPolarisPrincipal authenticatedPrincipal;

    public ContextVariableReplacementVisitor(AuthenticatedPolarisPrincipal authenticatedPrincipal) {
      this.authenticatedPrincipal = authenticatedPrincipal;
    }

    @Override
    public <T> Expression predicate(UnboundPredicate<T> pred) {
      String refName = pred.ref().name();

      if ("$current_principal_role".equals(refName)) {
        return evaluateCurrentPrincipalRole(pred);
      } else if ("$current_principal".equals(refName)) {
        return evaluateCurrentPrincipal(pred);
      }

      // Return the original predicate if it doesn't reference context variables
      return pred;
    }

    @Override
    public Expression alwaysTrue() {
      return Expressions.alwaysTrue();
    }

    @Override
    public Expression alwaysFalse() {
      return Expressions.alwaysFalse();
    }

    @Override
    public Expression not(Expression result) {
      return Expressions.not(result);
    }

    @Override
    public Expression and(Expression leftResult, Expression rightResult) {
      return Expressions.and(leftResult, rightResult);
    }

    @Override
    public Expression or(Expression leftResult, Expression rightResult) {
      return Expressions.or(leftResult, rightResult);
    }

    private Expression evaluateCurrentPrincipalRole(UnboundPredicate<?> pred) {
      String val = (String) pred.literal().value();
      boolean containsRole = authenticatedPrincipal.getActivatedPrincipalRoleNames().contains(val);

      return getExpression(pred, containsRole);
    }

    private Expression evaluateCurrentPrincipal(UnboundPredicate<?> pred) {
      String val = (String) pred.literal().value();
      boolean principalMatches = authenticatedPrincipal.getName().equals(val);

      return getExpression(pred, principalMatches);
    }

    private Expression getExpression(UnboundPredicate<?> pred, boolean principalMatches) {
      if (pred.op().equals(Expression.Operation.EQ)) {
        return principalMatches ? Expressions.alwaysTrue() : Expressions.alwaysFalse();
      } else if (pred.op().equals(Expression.Operation.NOT_EQ)) {
        return principalMatches ? Expressions.alwaysFalse() : Expressions.alwaysTrue();
      } else {
        // For other operations, return the original predicate
        return pred;
      }
    }
  }
}

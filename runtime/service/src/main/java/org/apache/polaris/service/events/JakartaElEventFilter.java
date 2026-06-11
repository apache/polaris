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
package org.apache.polaris.service.events;

import jakarta.el.ELContext;
import jakarta.el.ELManager;
import jakarta.el.ELResolver;
import jakarta.el.PropertyNotFoundException;
import jakarta.el.PropertyNotWritableException;
import jakarta.el.ValueExpression;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * An {@link EventFilter} that uses the Jakarta Expression Language to evaluate boolean predicates
 * on the event object.
 *
 * @see <a
 *     href="https://jakarta.ee/specifications/expression-language/6.0/jakarta-expression-language-spec-6.0">Jakarta
 *     Expression Language 6.0</a>
 */
public class JakartaElEventFilter implements EventFilter {
  public static final String TYPE = "jakarta-el";

  private static final EventAttributeMapResolver ATTRIBUTE_MAP_RESOLVER =
      new EventAttributeMapResolver();

  private final @Nullable ValueExpression includeExpression;
  private final @Nullable ValueExpression excludeExpression;

  public JakartaElEventFilter(@Nullable String include, @Nullable String exclude) {
    if (include == null && exclude == null) {
      throw new IllegalArgumentException("At least one include or exclude expression is required");
    }
    var expressionFactory = ELManager.getExpressionFactory();
    includeExpression =
        include == null
            ? null
            : expressionFactory.createValueExpression(
                createContext(null), expressionSource(include), Boolean.class);
    excludeExpression =
        exclude == null
            ? null
            : expressionFactory.createValueExpression(
                createContext(null), expressionSource(exclude), Boolean.class);
  }

  @Override
  public boolean test(PolarisEvent event) {
    var context = createContext(event);
    return matches(includeExpression, context, true) && !matches(excludeExpression, context, false);
  }

  private static boolean matches(
      @Nullable ValueExpression expression, ELContext context, boolean defaultValue) {
    return expression == null ? defaultValue : Boolean.TRUE.equals(expression.getValue(context));
  }

  private static ELContext createContext(@Nullable PolarisEvent event) {
    var manager = new ELManager();
    manager.addELResolver(ATTRIBUTE_MAP_RESOLVER);
    if (event != null) {
      manager.defineBean("type", event.type());
      manager.defineBean("metadata", event.metadata());
      manager.defineBean("attributes", event.attributes());
    }
    return manager.getELContext();
  }

  private static String expressionSource(String expression) {
    return expression.startsWith("${") || expression.startsWith("#{")
        ? expression
        : "${" + expression + "}";
  }

  private static final class EventAttributeMapResolver extends ELResolver {

    @Override
    public Object getValue(ELContext context, Object base, Object property) {
      if (base instanceof EventAttributeMap attributeMap) {
        context.setPropertyResolved(base, property);
        var attributeKey = attributeKey(property);
        return attributeMap.get(attributeKey).orElse(null);
      }
      return null;
    }

    @Override
    public Class<?> getType(ELContext context, Object base, Object property) {
      if (base instanceof EventAttributeMap) {
        context.setPropertyResolved(base, property);
        var attributeKey = attributeKey(property);
        return attributeKey.type().getRawType();
      }
      return null;
    }

    @Override
    public boolean isReadOnly(ELContext context, Object base, Object property) {
      if (base instanceof EventAttributeMap) {
        context.setPropertyResolved(base, property);
        return true;
      }
      return false;
    }

    @Override
    public void setValue(ELContext context, Object base, Object property, Object value) {
      if (base instanceof EventAttributeMap) {
        context.setPropertyResolved(base, property);
        throw new PropertyNotWritableException("Event attributes are read-only");
      }
    }

    @Override
    public Class<?> getCommonPropertyType(ELContext context, Object base) {
      return base instanceof EventAttributeMap ? Object.class : null;
    }

    private static AttributeKey<?> attributeKey(Object property) {
      return Optional.ofNullable(property)
          .map(Object::toString)
          .flatMap(EventAttributes::findByName)
          .orElseThrow(
              () -> new PropertyNotFoundException("No event attribute with name: " + property));
    }
  }
}

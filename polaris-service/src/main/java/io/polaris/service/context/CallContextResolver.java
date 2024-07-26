package io.polaris.service.context;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;
import io.polaris.core.context.CallContext;
import io.polaris.core.context.RealmContext;
import io.polaris.service.config.HasEntityManagerFactory;
import java.util.Map;

/** Uses the resolved RealmContext to further resolve elements of the CallContext. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface CallContextResolver extends HasEntityManagerFactory, Discoverable {
  CallContext resolveCallContext(
      RealmContext realmContext,
      String method,
      String path,
      Map<String, String> queryParams,
      Map<String, String> headers);
}

package org.apache.polaris.service.events.json.mixins;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.iceberg.catalog.TableIdentifier;

public class ObjectMapperFactory {
    private ObjectMapperFactory() {}

    public static ObjectMapper create(){
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new Jdk8Module()) // If you never serialize Optional, you can remove the .registerModule(new Jdk8Module()) line.
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                .disable(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS);

        mapper.addMixIn(PolarisEvent.class, PolarisEventBaseMixin.class);
        mapper.addMixIn(IcebergRestCatalogEvents.AfterRefreshTableEvent.class, AfterRefreshTableEventMixin.class);
        mapper.addMixIn(TableIdentifier.class, IcebergThirdPartyMixins.TableIdentifierMixin.class);
        mapper.addMixIn(Namespace.class, IcebergThirdPartyMixins.NamespaceMixin.class);


        return mapper;
    }

}

package org.apache.polaris.service.events.json.serde;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.iceberg.catalog.TableIdentifier;

import java.io.IOException;


public class TableIdentifierToStringSerializer extends JsonSerializer<TableIdentifier> {

    @Override
    public void serialize(TableIdentifier value, JsonGenerator gen, SerializerProvider serializers) throws IOException {

        if (value == null) {
            gen.writeNull();
            return;
        }
        gen.writeString(value.toString());

    }
}

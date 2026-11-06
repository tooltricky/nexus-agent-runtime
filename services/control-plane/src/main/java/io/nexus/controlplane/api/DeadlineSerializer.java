package io.nexus.controlplane.api;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.nexus.controlplane.domain.DeadlineTimestamp;

import java.io.IOException;
import java.time.Instant;

public class DeadlineSerializer extends JsonSerializer<Instant> {
    @Override
    public void serialize(Instant value, JsonGenerator generator, SerializerProvider serializers) throws IOException {
        generator.writeString(DeadlineTimestamp.format(value));
    }
}

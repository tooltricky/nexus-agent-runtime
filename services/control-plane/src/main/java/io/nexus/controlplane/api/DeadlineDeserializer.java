package io.nexus.controlplane.api;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

/** Deadlines accept only an offset timestamp, never epoch numbers or a local wall clock. */
public class DeadlineDeserializer extends JsonDeserializer<Instant> {
    private static final java.util.regex.Pattern TIMESTAMP = java.util.regex.Pattern.compile(
            "(?i)(?!0000)[0-9]{4}-[0-9]{2}-[0-9]{2}t[0-9]{2}:[0-9]{2}:[0-9]{2}"
                    + "(?:\\.[0-9]{1,9})?(z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])");
    @Override
    public Instant deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        if (parser.currentToken() == JsonToken.VALUE_STRING) {
            String text = parser.getText();
            var timestamp = TIMESTAMP.matcher(text);
            if (!timestamp.matches()) return (Instant) context.handleUnexpectedToken(Instant.class, parser);
            try {
                String offset = timestamp.group(1);
                int offsetSeconds = 0;
                if (!offset.equalsIgnoreCase("z")) {
                    offsetSeconds = (Integer.parseInt(offset.substring(1, 3)) * 60
                            + Integer.parseInt(offset.substring(4, 6))) * 60;
                    if (offset.charAt(0) == '-') offsetSeconds = -offsetSeconds;
                }
                // RFC 3339 allows offsets through +/-23:59, beyond ZoneOffset's +/-18h.
                // Parse the calendar strictly, then apply the independently validated offset.
                return LocalDateTime.parse(text.substring(0, timestamp.start(1)))
                        .toInstant(ZoneOffset.UTC).minusSeconds(offsetSeconds).truncatedTo(ChronoUnit.MICROS);
            } catch (DateTimeParseException ignored) { }
        }
        return (Instant) context.handleUnexpectedToken(Instant.class, parser);
    }
}

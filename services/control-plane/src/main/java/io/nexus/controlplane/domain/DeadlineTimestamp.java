package io.nexus.controlplane.domain;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Keep RFC 3339's four-digit local year even when UTC normalization crosses a year boundary. */
public final class DeadlineTimestamp {
    private static final Instant FIRST_UTC = Instant.parse("0001-01-01T00:00:00Z");
    private static final Instant AFTER_LAST_UTC = Instant.parse("+10000-01-01T00:00:00Z");
    private static final int MAX_OFFSET_SECONDS = 23 * 3600 + 59 * 60;

    private DeadlineTimestamp() { }

    public static String format(Instant value) {
        if (value.isBefore(FIRST_UTC)) {
            return local(value.plusSeconds(MAX_OFFSET_SECONDS)) + "+23:59";
        }
        if (!value.isBefore(AFTER_LAST_UTC)) {
            return local(value.minusSeconds(MAX_OFFSET_SECONDS)) + "-23:59";
        }
        return value.toString();
    }

    private static String local(Instant value) {
        return value.atOffset(ZoneOffset.UTC).toLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }
}

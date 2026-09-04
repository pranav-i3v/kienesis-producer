package com.pranav.kpl.model;

import java.time.Instant;
import java.util.UUID;

/** Versioned JSON envelope shared with the consumer's wire contract. */
public record KinesisRecord(
        UUID eventId,
        String eventType,
        String schemaVersion,
        Instant occurredAt,
        String partitionKey,
        Object data) {

    public KinesisRecord(String partitionKey, Object data) {
        this(UUID.randomUUID(), "generic", "1", Instant.now(), partitionKey, data);
    }

    public String getPartitionKey() {
        return partitionKey;
    }
}

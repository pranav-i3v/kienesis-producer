package com.pranav.kpl.model;

import java.util.UUID;

public record PublishResult(
        UUID eventId,
        boolean successful,
        String shardId,
        String sequenceNumber,
        int attempts,
        String errorCode,
        String errorMessage) {
}

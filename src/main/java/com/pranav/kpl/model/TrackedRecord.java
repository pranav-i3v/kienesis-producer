package com.pranav.kpl.model;

public record TrackedRecord(int index, String streamName, KinesisRecord record, byte[] serializedData,
                            int priorAttempts) {

    public TrackedRecord withPriorAttempts(int attempts) {
        return new TrackedRecord(index, streamName, record, serializedData, attempts);
    }
}

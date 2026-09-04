package com.pranav.kpl.model;

/** Result of a Kinesis control-plane stream health check. */
public record ProducerHealthResult(
        String streamName,
        boolean healthy,
        String streamStatus,
        String errorCode,
        String errorMessage) {
}

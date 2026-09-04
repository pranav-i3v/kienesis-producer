package com.pranav.kpl.service;

import com.pranav.kpl.model.FailureDetails;
import com.pranav.kpl.model.ProducerHealthResult;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.util.concurrent.CompletableFuture;

public class KinesisProducerHealthService {

    private final KinesisAsyncClient kinesisAsyncClient;

    public KinesisProducerHealthService(KinesisAsyncClient kinesisAsyncClient) {
        this.kinesisAsyncClient = kinesisAsyncClient;
    }

    /**
     * Performs a health check for the specified Kinesis stream.
     *
     * <p>This method uses the {@link KinesisAsyncClient} to perform a
     * Kinesis control-plane operation and verifies that the stream is
     * currently in the {@code ACTIVE} state. It does not publish any
     * records and does not use the KPL producer for the health check.</p>
     *
     * <p>The check is performed asynchronously and returns a
     * {@link CompletableFuture} containing the health status of the stream.</p>
     *
     * <p>If the stream name is missing or blank, the method immediately
     * returns a failed health result without making an AWS API call.</p>
     *
     * <p>If AWS returns an error while describing the stream, the exception
     * is converted into {@link FailureDetails} and returned as part of the
     * health result rather than completing the future exceptionally.</p>
     *
     * <p>The stream is considered healthy only when its status is
     * {@code ACTIVE}. Other valid Kinesis stream states, such as
     * {@code CREATING}, {@code UPDATING}, or {@code DELETING}, result in
     * an unhealthy response.</p>
     *
     * @param streamName name of the Kinesis stream to check
     * @return a {@link CompletableFuture} containing the stream health status,
     *         current stream status, and error details when applicable
     */
    public CompletableFuture<ProducerHealthResult> healthCheck(String streamName) {
        if (streamName == null || streamName.isBlank()) {
            return CompletableFuture.completedFuture(new ProducerHealthResult(streamName, false, null,
                    "INVALID_STREAM_NAME", "streamName is required"));
        }
        return kinesisAsyncClient.describeStreamSummary(builder -> builder.streamName(streamName))
                .handle((response, error) -> {
                    if (error != null) {
                        FailureDetails details = FailureDetails.from(error);
                        return new ProducerHealthResult(streamName, false, null, details.code(), details.message());
                    }
                    String status = response.streamDescriptionSummary().streamStatusAsString();
                    return new ProducerHealthResult(streamName, "ACTIVE".equals(status), status, null, null);
                });
    }
}

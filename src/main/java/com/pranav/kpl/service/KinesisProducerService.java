package com.pranav.kpl.service;

import com.pranav.kpl.config.KinesisProducerProperties;
import com.pranav.kpl.model.AttemptOutcome;
import com.pranav.kpl.internal.KinesisRecordValidator;
import com.pranav.kpl.internal.KinesisRetryPolicy;
import com.pranav.kpl.internal.KplRecordPublisher;
import com.pranav.kpl.model.PreparedBatch;
import com.pranav.kpl.model.PreparedRecord;
import com.pranav.kpl.model.TrackedRecord;
import com.pranav.kpl.util.JsonUtil;
import com.pranav.kpl.model.BatchPublishResult;
import com.pranav.kpl.model.ProducerHealthResult;
import com.pranav.kpl.model.PublishResult;
import com.pranav.kpl.request.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Publishes data asynchronously through AWS SDK v2 KinesisAsyncClient. */
public class KinesisProducerService {

    private final KinesisRecordValidator recordValidator;
    private final KplRecordPublisher recordPublisher;
    private final KinesisRetryPolicy retryPolicy;
    private final KinesisProducerHealthService healthService;

    public KinesisProducerService(KinesisAsyncClient kinesisAsyncClient,
                                  JsonUtil jsonUtil,
                                  KinesisProducerProperties properties) {
        this(new KinesisRecordValidator(jsonUtil, properties),
                new KplRecordPublisher(kinesisAsyncClient),
                new KinesisRetryPolicy(properties),
                new KinesisProducerHealthService(kinesisAsyncClient));
    }

    public KinesisProducerService(KinesisRecordValidator recordValidator,
                                  KplRecordPublisher recordPublisher,
                                  KinesisRetryPolicy retryPolicy,
                                  KinesisProducerHealthService healthService) {
        this.recordValidator = recordValidator;
        this.recordPublisher = recordPublisher;
        this.retryPolicy = retryPolicy;
        this.healthService = healthService;
    }

    /**
     * Publishes a single record synchronously to Kinesis.
     *
     * <p>This is the synchronous wrapper around {@link #putRecordAsync(PutRecordRequest)}.
     * The calling thread waits until record validation, submission, and any configured
     * retry processing are completed.</p>
     *
     * @param request record publish request containing the stream, partition key,
     *                and payload
     * @return the result of the publish operation
     */
    public PublishResult putRecord(PutRecordRequest request) {
        return putRecordAsync(request).join();
    }

    /**
     * Publishes a single record asynchronously to Kinesis.
     *
     * <p>The request is first validated and prepared by {@code recordValidator}.
     * Invalid requests are completed immediately with a failed {@link PublishResult}.
     * Valid records are passed to {@code recordPublisher} for KPL submission.</p>
     *
     * <p>The returned future completes when the record submission finishes.
     * Retry handling is performed by the publisher/retry infrastructure when
     * applicable.</p>
     *
     * @param request record publish request containing the stream, partition key,
     *                and payload
     * @return a future containing the publish result
     */
    public CompletableFuture<PublishResult> putRecordAsync(PutRecordRequest request) {
        PreparedRecord prepared = recordValidator.prepareSingle(request);
        if (prepared.failure() != null) {
            return CompletableFuture.completedFuture(prepared.failure());
        }
        return recordPublisher.submit(prepared.trackedRecord()).thenApply(AttemptOutcome::result);
    }

    /**
     * Publishes a batch of records synchronously to Kinesis.
     *
     * <p>This is the synchronous wrapper around
     * {@link #putRecordBatchAsync(PutRecordRequest)}.</p>
     *
     * @param request batch publish request
     * @return batch publish result containing the result for each record
     */
    public BatchPublishResult putRecordBatch(PutRecordRequest request) {
        return putRecordBatchAsync(request).join();
    }

    /**
     * Publishes a batch of records asynchronously to Kinesis.
     *
     * <p>The batch is first validated and prepared by {@code recordValidator}.
     * Invalid records are recorded in the result while valid records continue
     * through the publishing pipeline.</p>
     *
     * <p>Valid records are submitted together through {@code recordPublisher}.
     * Failed records are then evaluated by the retry policy. Only failures that
     * satisfy the configured retry policy are resubmitted. The process continues
     * until there are no retryable failures remaining or the retry limit is reached.</p>
     *
     * <p>The final {@link BatchPublishResult} preserves the result for each
     * original record using its original index.</p>
     *
     * @param request batch publish request
     * @return a future containing the result of each record in the batch
     */
    public CompletableFuture<BatchPublishResult> putRecordBatchAsync(PutRecordRequest request) {
        PreparedBatch prepared = recordValidator.prepareBatch(request);
        if (prepared.validRecords().isEmpty()) {
            return CompletableFuture.completedFuture(prepared.result());
        }
        return recordPublisher.submitAll(prepared.validRecords())
                .thenCompose(outcomes -> retryFailed(outcomes, prepared.resultsByIndex(), 0))
                .thenApply(ignored -> prepared.result());
    }

    /**
     * Performs a Kinesis stream health check.
     *
     * <p>The health check is delegated to {@code healthService}, which uses
     * the AWS SDK Kinesis client for control-plane operations rather than
     * the KPL producer.</p>
     *
     * @param streamName name of the Kinesis stream to check
     * @return a future containing the stream health status
     */
    public CompletableFuture<ProducerHealthResult> healthCheck(String streamName) {
        return healthService.healthCheck(streamName);
    }

    /**
     * Retries failed records according to the configured retry policy.
     *
     * <p>Each failed attempt is first stored in {@code resultsByIndex} so that
     * the final batch result retains the outcome for every original record.</p>
     *
     * <p>Only records for which {@code retryPolicy.shouldRetry(...)} returns
     * {@code true} are submitted again. A configurable delay, including the
     * retry backoff strategy, is applied before each retry round.</p>
     *
     * <p>The retry process continues recursively until there are no retryable
     * records remaining or the configured maximum retry count is reached.</p>
     *
     * @param outcomes results from the current submission attempt
     * @param resultsByIndex map used to maintain results in their original
     *                       batch order
     * @param completedRetries number of retry rounds already completed
     * @return a future that completes when all retry processing is finished
     */
    private CompletableFuture<Void> retryFailed(List<AttemptOutcome> outcomes,
                                                Map<Integer, PublishResult> resultsByIndex,
                                                int completedRetries) {
        List<TrackedRecord> retryable = new ArrayList<>();
        for (AttemptOutcome outcome : outcomes) {
            resultsByIndex.put(outcome.record().index(), outcome.result());
            if (retryPolicy.shouldRetry(outcome, completedRetries)) {
                retryable.add(outcome.record().withPriorAttempts(outcome.result().attempts()));
            }
        }
        if (retryable.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        long delay = retryPolicy.retryDelay(completedRetries + 1);
        return CompletableFuture.runAsync(() -> { }, CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS))
                .thenCompose(ignored -> recordPublisher.submitAll(retryable))
                .thenCompose(next -> retryFailed(next, resultsByIndex, completedRetries + 1));
    }
}

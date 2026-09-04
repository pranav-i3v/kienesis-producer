package com.pranav.kpl.internal;

import com.google.common.util.concurrent.ListenableFuture;
import com.pranav.kpl.model.AttemptOutcome;
import com.pranav.kpl.model.FailureDetails;
import com.pranav.kpl.model.PublishResult;
import com.pranav.kpl.model.TrackedRecord;
import software.amazon.kinesis.producer.Attempt;
import software.amazon.kinesis.producer.IKinesisProducer;
import software.amazon.kinesis.producer.UserRecordResult;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class KplRecordPublisher {

    private final IKinesisProducer kinesisProducer;

    public KplRecordPublisher(IKinesisProducer kinesisProducer) {
        this.kinesisProducer = kinesisProducer;
    }

    public CompletableFuture<List<AttemptOutcome>> submitAll(List<TrackedRecord> records) {
        List<CompletableFuture<AttemptOutcome>> submissions = records.stream().map(this::submit).toList();
        return CompletableFuture.allOf(submissions.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> submissions.stream().map(CompletableFuture::join).toList());
    }

    public CompletableFuture<AttemptOutcome> submit(TrackedRecord record) {
        try {
            ListenableFuture<UserRecordResult> future = kinesisProducer.addUserRecord(
                    record.streamName(), record.record().partitionKey(), ByteBuffer.wrap(record.serializedData()));
            return toCompletableFuture(future).handle((result, error) -> new AttemptOutcome(record,
                    error == null ? mapResult(record, result) : mapFailure(record, error)));
        } catch (RuntimeException error) {
            return CompletableFuture.completedFuture(new AttemptOutcome(record, mapFailure(record, error)));
        }
    }

    private CompletableFuture<UserRecordResult> toCompletableFuture(ListenableFuture<UserRecordResult> future) {
        CompletableFuture<UserRecordResult> result = new CompletableFuture<>();
        future.addListener(() -> {
            try {
                result.complete(future.get());
            } catch (Exception error) {
                result.completeExceptionally(error);
            }
        }, Runnable::run);
        return result;
    }

    private PublishResult mapResult(TrackedRecord record, UserRecordResult response) {
        int attempts = record.priorAttempts() + response.getAttempts().size();
        if (response.isSuccessful()) {
            return new PublishResult(record.record().eventId(), true, response.getShardId(),
                    response.getSequenceNumber(), attempts, null, null);
        }
        Attempt finalAttempt = response.getAttempts().isEmpty() ? null
                : response.getAttempts().get(response.getAttempts().size() - 1);
        return KinesisRecordValidator.failure(record.record().eventId(),
                finalAttempt == null ? "KPL_RECORD_FAILED" : finalAttempt.getErrorCode(),
                finalAttempt == null ? "KPL did not provide failure details" : finalAttempt.getErrorMessage(),
                attempts);
    }

    private PublishResult mapFailure(TrackedRecord record, Throwable error) {
        FailureDetails details = FailureDetails.from(error);
        return KinesisRecordValidator.failure(record.record().eventId(), details.code(), details.message(),
                record.priorAttempts() + 1);
    }
}

package com.pranav.kpl.internal;

import com.pranav.kpl.model.AttemptOutcome;
import com.pranav.kpl.model.FailureDetails;
import com.pranav.kpl.model.PublishResult;
import com.pranav.kpl.model.TrackedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class KplRecordPublisher {

    private static final Logger logger = LoggerFactory.getLogger(KplRecordPublisher.class);

    private final KinesisAsyncClient kinesisAsyncClient;

    public KplRecordPublisher(KinesisAsyncClient kinesisAsyncClient) {
        this.kinesisAsyncClient = kinesisAsyncClient;
    }

    public CompletableFuture<List<AttemptOutcome>> submitAll(List<TrackedRecord> records) {
        if (records == null || records.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }

        String streamName = records.get(0).streamName();
        boolean singleStream = records.stream().allMatch(r -> streamName.equals(r.streamName()));

        if (singleStream) {
            List<PutRecordsRequestEntry> entries = records.stream()
                    .map(r -> PutRecordsRequestEntry.builder()
                            .partitionKey(r.record().partitionKey())
                            .data(SdkBytes.fromByteArray(r.serializedData()))
                            .build())
                    .toList();

            PutRecordsRequest request = PutRecordsRequest.builder()
                    .streamName(streamName)
                    .records(entries)
                    .build();

            return kinesisAsyncClient.putRecords(request)
                    .handle((response, error) -> {
                        if (error != null) {
                            return records.stream()
                                    .map(r -> new AttemptOutcome(r, mapFailure(r, error)))
                                    .toList();
                        }
                        List<AttemptOutcome> outcomes = new ArrayList<>(records.size());
                        List<PutRecordsResultEntry> resultEntries = response.records();
                        for (int i = 0; i < records.size(); i++) {
                            TrackedRecord record = records.get(i);
                            PutRecordsResultEntry entry = i < resultEntries.size() ? resultEntries.get(i) : null;
                            outcomes.add(new AttemptOutcome(record, mapEntryResult(record, entry)));
                        }
                        return outcomes;
                    });
        }

        List<CompletableFuture<AttemptOutcome>> submissions = records.stream().map(this::submit).toList();
        return CompletableFuture.allOf(submissions.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> submissions.stream().map(CompletableFuture::join).toList());
    }

    public CompletableFuture<AttemptOutcome> submit(TrackedRecord record) {
        try {
            PutRecordRequest request = PutRecordRequest.builder()
                    .streamName(record.streamName())
                    .partitionKey(record.record().partitionKey())
                    .data(SdkBytes.fromByteArray(record.serializedData()))
                    .build();

            return kinesisAsyncClient.putRecord(request)
                    .handle((response, error) -> new AttemptOutcome(record,
                            error == null ? mapResult(record, response) : mapFailure(record, error)));
        } catch (RuntimeException error) {
            return CompletableFuture.completedFuture(new AttemptOutcome(record, mapFailure(record, error)));
        }
    }

    private PublishResult mapResult(TrackedRecord record, PutRecordResponse response) {
        int attempts = record.priorAttempts() + 1;
        return new PublishResult(record.record().eventId(), true, response.shardId(),
                response.sequenceNumber(), attempts, null, null);
    }

    private PublishResult mapEntryResult(TrackedRecord record, PutRecordsResultEntry entry) {
        int attempts = record.priorAttempts() + 1;
        if (entry == null) {
            return KinesisRecordValidator.failure(record.record().eventId(),
                    "KINESIS_RECORD_FAILED", "Kinesis did not provide result entry", attempts);
        }
        if (entry.errorCode() == null || entry.errorCode().isBlank()) {
            logger.info("Record {} published successfully to shard {} with sequence number {}",
                    record.record().eventId(), entry.shardId(), entry.sequenceNumber());
            return new PublishResult(record.record().eventId(), true, entry.shardId(),
                    entry.sequenceNumber(), attempts, null, null);
        }
        return KinesisRecordValidator.failure(record.record().eventId(),
                entry.errorCode(), entry.errorMessage(), attempts);
    }

    private PublishResult mapFailure(TrackedRecord record, Throwable error) {
        FailureDetails details = FailureDetails.from(error);
        return KinesisRecordValidator.failure(record.record().eventId(), details.code(), details.message(),
                record.priorAttempts() + 1);
    }
}

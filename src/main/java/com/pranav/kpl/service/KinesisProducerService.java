package com.pranav.kpl.service;

import com.pranav.kpl.config.KinesisConfig;
import com.pranav.kpl.exception.KinesisProducerException;
import com.pranav.kpl.model.KinesisRecord;
import com.pranav.kpl.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class KinesisProducerService {
    private static final Logger logger = LoggerFactory.getLogger(KinesisProducerService.class);

    private final KinesisClient kinesisClient;
    private final JsonUtil jsonUtil;
    private final RetryService retryService;

    public KinesisProducerService(KinesisClient kinesisClient, JsonUtil jsonUtil,
                                  KinesisConfig.KinesisProperties kinesisProperties, RetryService retryService) {
        this.kinesisClient = kinesisClient;
        this.jsonUtil = jsonUtil;
        this.retryService = retryService;
    }

    /**
     * Send a single record to Kinesis stream (blocking)
     * @param streamName Kinesis stream name
     * @param partitionKey Partition key for the record
     * @param data Record payload
     */
    public void putRecord(String streamName, String partitionKey, Object data) {
        try {
            KinesisRecord record = new KinesisRecord(null, partitionKey, data);
            byte[] serializedData = jsonUtil.toJsonBytes(record);

            PutRecordRequest request = PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(partitionKey)
                .data(SdkBytes.fromByteArray(serializedData))
                .build();

            PutRecordResponse response = retryService.executeWithRetry(() -> {
                logger.debug("Putting record to stream: {}, partition key: {}", 
                    streamName, partitionKey);
                return kinesisClient.putRecord(request);
            }, "putRecord");

            logger.info("Record sent successfully. Stream: {}, Sequence: {}, Shard: {}", 
                streamName, response.sequenceNumber(), response.shardId());

        } catch (Exception e) {
            logger.error("Failed to put record to stream: {} with partition key: {}", streamName, partitionKey, e);
            throw new KinesisProducerException("Failed to put record", "PUT_RECORD_ERROR", true, e);
        }
    }

    /**
     * Send a single record to Kinesis stream (async)
     * @param streamName Kinesis stream name
     * @param partitionKey Partition key for the record
     * @param data Record payload
     * @return CompletableFuture
     */
    @Async("kinesisExecutor")
    public CompletableFuture<Void> putRecordAsync(String streamName, String partitionKey, Object data) {
        return CompletableFuture.runAsync(() -> putRecord(streamName, partitionKey, data));
    }

    /**
     * Send multiple records to Kinesis stream in a batch (blocking)
     * @param streamName Kinesis stream name
     * @param records List of KinesisRecord objects
     */
    public void putRecordBatch(String streamName, List<KinesisRecord> records) {
        if (records == null || records.isEmpty()) {
            logger.warn("Empty records list provided to putRecordBatch");
            return;
        }

        try {
            List<PutRecordsRequestEntry> entries = new ArrayList<>();
            for (KinesisRecord record : records) {
                byte[] serializedData = jsonUtil.toJsonBytes(record);
                PutRecordsRequestEntry entry = PutRecordsRequestEntry.builder()
                    .partitionKey(record.getPartitionKey())
                    .data(SdkBytes.fromByteArray(serializedData))
                    .build();
                entries.add(entry);
            }

            PutRecordsRequest request = PutRecordsRequest.builder()
                .streamName(streamName)
                .records(entries)
                .build();

            PutRecordsResponse response = retryService.executeWithRetry(() -> {
                logger.debug("Putting batch of {} records to stream: {}", 
                    entries.size(), streamName);
                return kinesisClient.putRecords(request);
            }, "putRecordBatch");

            logger.info("Batch sent successfully. Stream: {}, Total records: {}, Failed: {}", 
                streamName, records.size(), response.failedRecordCount());

            if (response.failedRecordCount() > 0) {
                logger.warn("Some records failed in batch. Failed count: {}", 
                    response.failedRecordCount());
            }

        } catch (Exception e) {
            logger.error("Failed to put batch of {} records to stream: {}", records.size(), streamName, e);
            throw new KinesisProducerException("Failed to put batch", "BATCH_ERROR", true, e);
        }
    }

    /**
     * Send multiple records to Kinesis stream in a batch (async)
     * @param streamName Kinesis stream name
     * @param records List of KinesisRecord objects
     * @return CompletableFuture
     */
    @Async("kinesisExecutor")
    public CompletableFuture<Void> putRecordBatchAsync(String streamName, List<KinesisRecord> records) {
        return CompletableFuture.runAsync(() -> putRecordBatch(streamName, records));
    }

    /**
     * Close the Kinesis client connection
     */
    public void close() {
        try {
            logger.info("Closing Kinesis client");
            kinesisClient.close();
        } catch (Exception e) {
            logger.error("Error closing Kinesis client", e);
        }
    }
}


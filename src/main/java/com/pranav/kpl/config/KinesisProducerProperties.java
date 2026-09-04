package com.pranav.kpl.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Set;

@ConfigurationProperties(prefix = "aws.kinesis.producer")
public class KinesisProducerProperties {

    private boolean enabled = true;

    /**
     * AWS region where the Kinesis stream is located.
     * Example: us-east-1
     */
    private String region = "us-east-1";

    /**
     * Name of the Kinesis Data Stream to which records are published.
     */
    private String streamName;

    /**
     * Maximum number of HTTP connections that can be used at the same time.
     * If all connections are busy, new requests wait for an available connection.
     */
    private int maxConnections = 24;

    /**
     * Maximum time allowed for a Kinesis request to complete, in milliseconds.
     * The request fails if it does not complete within this time.
     */
    private long requestTimeout = 60000;

    /**
     * Maximum time, in milliseconds, that a record can stay in the producer buffer
     * while waiting for more records to be collected.
     * <p>
     * Lower values reduce latency but may result in smaller batches.
     */
    private long recordMaxBufferedTime = 100;

    /**
     * Maximum number of records that can be sent in a single Kinesis batch request.
     * <p>
     * Example: 1,200 records are sent as 3 batches:
     * 500 + 500 + 200.
     */
    private int maxRecordsPerBatch = 500;

    /**
     * Enables or disables record aggregation.
     * <p>
     * When enabled, multiple small user records can be combined into a single
     * Kinesis record before being sent. This can improve throughput and reduce
     * the number of Kinesis records sent.
     */
    private boolean aggregationEnabled = true;

    /**
     * Maximum size, in bytes, of a single aggregated Kinesis record.
     * <p>
     * Aggregation stops when this size limit is reached.
     */
    private int aggregationMaxSize = 51200;

    /**
     * Maximum number of user records that can be included in a single
     * aggregated Kinesis record.
     * <p>
     * Aggregation stops when this count limit is reached.
     */
    private int aggregationMaxCount = 100;

    /**
     * Maximum number of times a failed Kinesis operation is retried.
     */
    private int maxRetries = 3;

    /**
     * Initial delay before the first retry, in milliseconds.
     */
    private long retryWaitTimeMs = 1000;

    /**
     * Multiplier used to increase the delay between retry attempts.
     * <p>
     * With 1000 ms initial delay and a multiplier of 2.0:
     * 1st retry: 1000 ms
     * 2nd retry: 2000 ms
     * 3rd retry: 4000 ms
     */
    private double backoffMultiplier = 2.0;

    /** Maximum full-jitter retry delay in milliseconds. */
    private long retryMaxDelayMs = 30000;

    /**
     * Maximum size of a single application record in bytes.
     */
    private int maxRecordSizeBytes = 1024 * 1024;

    /**
     * Maximum number of records allowed in a single batch request.
     */
    private int maxBatchRecords = 500;

    /**
     * Maximum total payload size allowed for a batch request in bytes.
     */
    private int maxBatchPayloadBytes = 5 * 1024 * 1024;

    /**
     * AWS error codes that are considered retryable by the producer.
     */
    private Set<String> retryableErrorCodes = Set.of(
            "ProvisionedThroughputExceededException",
            "LimitExceededException",
            "InternalFailure",
            "InternalFailureException",
            "ServiceUnavailable",
            "ServiceUnavailableException",
            "RequestTimeout",
            "RequestTimeoutException",
            "KMSDisabledException",
            "KMSInvalidStateException",
            "KMSThrottlingException"
    );

    public int getMaxRecordSizeBytes() {
        return maxRecordSizeBytes;
    }

    public void setMaxRecordSizeBytes(int maxRecordSizeBytes) {
        this.maxRecordSizeBytes = maxRecordSizeBytes;
    }

    public int getMaxBatchRecords() {
        return maxBatchRecords;
    }

    public void setMaxBatchRecords(int maxBatchRecords) {
        this.maxBatchRecords = maxBatchRecords;
    }

    public int getMaxBatchPayloadBytes() {
        return maxBatchPayloadBytes;
    }

    public void setMaxBatchPayloadBytes(int maxBatchPayloadBytes) {
        this.maxBatchPayloadBytes = maxBatchPayloadBytes;
    }

    public Set<String> getRetryableErrorCodes() {
        return retryableErrorCodes;
    }

    public void setRetryableErrorCodes(Set<String> retryableErrorCodes) {
        this.retryableErrorCodes = retryableErrorCodes;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public long getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(long requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public long getRecordMaxBufferedTime() {
        return recordMaxBufferedTime;
    }

    public void setRecordMaxBufferedTime(long recordMaxBufferedTime) {
        this.recordMaxBufferedTime = recordMaxBufferedTime;
    }

    public int getMaxRecordsPerBatch() {
        return maxRecordsPerBatch;
    }

    public void setMaxRecordsPerBatch(int maxRecordsPerBatch) {
        this.maxRecordsPerBatch = maxRecordsPerBatch;
    }

    public boolean isAggregationEnabled() {
        return aggregationEnabled;
    }

    public void setAggregationEnabled(boolean aggregationEnabled) {
        this.aggregationEnabled = aggregationEnabled;
    }

    public int getAggregationMaxSize() {
        return aggregationMaxSize;
    }

    public void setAggregationMaxSize(int aggregationMaxSize) {
        this.aggregationMaxSize = aggregationMaxSize;
    }

    public int getAggregationMaxCount() {
        return aggregationMaxCount;
    }

    public void setAggregationMaxCount(int aggregationMaxCount) {
        this.aggregationMaxCount = aggregationMaxCount;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getRetryWaitTimeMs() {
        return retryWaitTimeMs;
    }

    public void setRetryWaitTimeMs(long retryWaitTimeMs) {
        this.retryWaitTimeMs = retryWaitTimeMs;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public long getRetryMaxDelayMs() {
        return retryMaxDelayMs;
    }

    public void setRetryMaxDelayMs(long retryMaxDelayMs) {
        this.retryMaxDelayMs = retryMaxDelayMs;
    }
}

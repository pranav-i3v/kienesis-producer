package com.pranav.kpl.internal;

import com.pranav.kpl.config.KinesisProducerProperties;
import com.pranav.kpl.model.*;
import com.pranav.kpl.request.PutRecordRequest;
import com.pranav.kpl.util.JsonUtil;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KinesisRecordValidator {

    private final JsonUtil jsonUtil;
    private final KinesisProducerProperties properties;

    public KinesisRecordValidator(JsonUtil jsonUtil, KinesisProducerProperties properties) {
        this.jsonUtil = jsonUtil;
        this.properties = properties;
    }

    public PreparedRecord prepareSingle(PutRecordRequest request) {
        if (request == null) {
            return new PreparedRecord(null, failure(null, "INVALID_REQUEST", "request is required", 0));
        }
        return prepare(request, new KinesisRecord(request.getPartitionKey(), request.getData()), 0, 0);
    }

    private PreparedRecord prepare(PutRecordRequest request, KinesisRecord record, int index, int priorAttempts) {
        UUID eventId = eventId(record);
        String streamName = request == null ? null : resolveStreamName(request.getStreamName());
        if (streamName == null || streamName.isBlank()) {
            return new PreparedRecord(null, failure(eventId, "INVALID_STREAM_NAME", "streamName is required", priorAttempts));
        }
        if (record == null || record.partitionKey() == null || record.partitionKey().isBlank()) {
            return new PreparedRecord(null, failure(eventId, "INVALID_PARTITION_KEY", "partitionKey is required", priorAttempts));
        }
        if (isEmptyPayload(record.data())) {
            return new PreparedRecord(null, failure(eventId, "INVALID_PAYLOAD", "payload is required", priorAttempts));
        }
        try {
            byte[] data = jsonUtil.toJsonBytes(record);
            if (data.length == 0 || data.length > properties.getMaxRecordSizeBytes()) {
                return new PreparedRecord(null, failure(eventId, "INVALID_RECORD_SIZE",
                        "A serialized record must be between 1 byte and " + properties.getMaxRecordSizeBytes() + " bytes",
                        priorAttempts));
            }
            return new PreparedRecord(new TrackedRecord(index, streamName, record, data, priorAttempts), null);
        } catch (RuntimeException error) {
            return new PreparedRecord(null, failure(eventId, "SERIALIZATION_ERROR", error.getMessage(), priorAttempts));
        }
    }

    public PreparedBatch prepareBatch(PutRecordRequest request) {
        List<KinesisRecord> records = request == null ? List.of() : request.getRecords();
        List<TrackedRecord> valid = new ArrayList<>();
        Map<Integer, PublishResult> resultsByIndex = new HashMap<>();

        if (records.size() > maximumBatchRecords()) {
            for (int index = 0; index < records.size(); index++) {
                resultsByIndex.put(index, failure(eventId(records.get(index)), "INVALID_BATCH_SIZE",
                        "A batch may contain at most " + maximumBatchRecords() + " logical records", 0));
            }
            return new PreparedBatch(records.size(), List.of(), resultsByIndex);
        }

        long totalPayloadBytes = 0;
        for (int index = 0; index < records.size(); index++) {
            PreparedRecord prepared = prepare(request, records.get(index), index, 0);
            if (prepared.failure() != null) {
                resultsByIndex.put(index, prepared.failure());
            } else {
                valid.add(prepared.trackedRecord());
                totalPayloadBytes += prepared.trackedRecord().serializedData().length;
            }
        }

        if (totalPayloadBytes > properties.getMaxBatchPayloadBytes()) {
            for (TrackedRecord record : valid) {
                resultsByIndex.put(record.index(), failure(record.record().eventId(), "INVALID_BATCH_SIZE",
                        "The total batch payload must not exceed " + properties.getMaxBatchPayloadBytes() + " bytes",
                        record.priorAttempts()));
            }
            return new PreparedBatch(records.size(), List.of(), resultsByIndex);
        }

        return new PreparedBatch(records.size(), valid, resultsByIndex);
    }

    private String resolveStreamName(String streamName) {
        if (streamName != null && !streamName.isBlank()) {
            return streamName;
        }
        return properties.getStreamName();
    }

    private boolean isEmptyPayload(Object data) {
        if (data == null) {
            return true;
        }
        if (data instanceof CharSequence text) {
            return text.isEmpty();
        }
        if (data instanceof java.util.Collection<?> collection) {
            return collection.isEmpty();
        }
        if (data instanceof Map<?, ?> map) {
            return map.isEmpty();
        }
        if (data.getClass().isArray()) {
            return Array.getLength(data) == 0;
        }
        return false;
    }

    private int maximumBatchRecords() {
        return Math.min(properties.getMaxBatchRecords(), Math.max(1, properties.getMaxRecordsPerBatch()));
    }

    private static UUID eventId(KinesisRecord record) {
        return record == null ? null : record.eventId();
    }

    static PublishResult failure(UUID eventId, String code, String message, int attempts) {
        return new PublishResult(eventId, false, null, null, attempts, code, message);
    }
}

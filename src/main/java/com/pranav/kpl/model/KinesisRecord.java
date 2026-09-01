package com.pranav.kpl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.time.LocalDateTime;

public class KinesisRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("record_id")
    private String recordId;

    @JsonProperty("partition_key")
    private String partitionKey;

    @JsonProperty("data")
    private Object data;

    @JsonProperty("timestamp")
    private LocalDateTime timestamp;

    @JsonProperty("sequence_number")
    private String sequenceNumber;

    @JsonProperty("shard_id")
    private String shardId;

    public KinesisRecord() {
    }

    public KinesisRecord(String recordId, String partitionKey, Object data) {
        this.recordId = recordId;
        this.partitionKey = partitionKey;
        this.data = data;
        this.timestamp = LocalDateTime.now();
    }

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(String sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }
}

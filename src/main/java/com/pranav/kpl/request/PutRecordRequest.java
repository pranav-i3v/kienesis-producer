package com.pranav.kpl.request;

import com.pranav.kpl.model.KinesisRecord;

import java.util.Collections;
import java.util.List;

/**
 * Input for sending one record to an AWS Kinesis stream.
 */
public class PutRecordRequest {
    private String streamName;
    private String partitionKey;
    private Object data;
    private List<Object> dataList;

    public PutRecordRequest() {
    }

    public PutRecordRequest(String streamName, String partitionKey, Object data) {
        this.streamName = streamName;
        this.partitionKey = partitionKey;
        this.data = data;
    }

    public PutRecordRequest(String streamName, String partitionKey, List<Object> dataList) {
        this.streamName = streamName;
        this.partitionKey = partitionKey;
        this.dataList = dataList;
    }

    public List<Object> getDataList() {
        return dataList;
    }

    public void setDataList(List<Object> dataList) {
        this.dataList = dataList;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
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

    public List<KinesisRecord> getRecords() {
        if (dataList != null && !dataList.isEmpty()) {
            return dataList.stream()
                .map(data -> new KinesisRecord(partitionKey, data))
                .toList();
        }
        return Collections.emptyList();
    }
}

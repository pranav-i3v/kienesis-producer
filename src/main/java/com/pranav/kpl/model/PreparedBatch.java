package com.pranav.kpl.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record PreparedBatch(int recordCount, List<TrackedRecord> validRecords,
                            Map<Integer, PublishResult> resultsByIndex) {

    public BatchPublishResult result() {
        List<PublishResult> results = new ArrayList<>(recordCount);
        for (int index = 0; index < recordCount; index++) {
            results.add(resultsByIndex.get(index));
        }
        return new BatchPublishResult(results);
    }
}

package com.pranav.kpl.model;

import java.util.List;

public record BatchPublishResult(List<PublishResult> records) {
    public long successfulCount() {
        return records.stream().filter(PublishResult::successful).count();
    }

    public long failedCount() {
        return records.size() - successfulCount();
    }
}

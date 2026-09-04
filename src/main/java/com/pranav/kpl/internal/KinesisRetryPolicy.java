package com.pranav.kpl.internal;

import com.pranav.kpl.config.KinesisProducerProperties;
import com.pranav.kpl.model.AttemptOutcome;

import java.util.concurrent.ThreadLocalRandom;

public class KinesisRetryPolicy {

    private final KinesisProducerProperties properties;

    public KinesisRetryPolicy(KinesisProducerProperties properties) {
        this.properties = properties;
    }

    public boolean shouldRetry(AttemptOutcome outcome, int completedRetries) {
        return !outcome.result().successful()
                && completedRetries < properties.getMaxRetries()
                && isRetryable(outcome.result().errorCode());
    }

    public long retryDelay(int retryRound) {
        double unbounded = properties.getRetryWaitTimeMs() * Math.pow(properties.getBackoffMultiplier(), retryRound - 1);
        long cap = Math.max(0, properties.getRetryMaxDelayMs());
        long upperBound = Math.min(cap, unbounded >= Long.MAX_VALUE ? Long.MAX_VALUE : (long) unbounded);
        if (upperBound == 0) {
            return 0;
        }
        return upperBound == Long.MAX_VALUE ? ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)
                : ThreadLocalRandom.current().nextLong(upperBound + 1);
    }

    private boolean isRetryable(String errorCode) {
        return errorCode != null && properties.getRetryableErrorCodes().contains(errorCode);
    }
}

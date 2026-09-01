package com.pranav.kpl.service;

import com.pranav.kpl.config.KinesisConfig;
import com.pranav.kpl.exception.KinesisProducerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class RetryService {
    private static final Logger logger = LoggerFactory.getLogger(RetryService.class);

    private final KinesisConfig.KinesisProperties properties;

    public RetryService(KinesisConfig.KinesisProperties properties) {
        this.properties = properties;
    }

    public <T> T executeWithRetry(Supplier<T> operation, String operationName) {
        int attempt = 0;
        long waitTime = properties.getRetryWaitTimeMs();
        Exception lastException = null;

        while (attempt < properties.getMaxRetries()) {
            try {
                logger.debug("Executing {} - attempt {}/{}", operationName, attempt + 1, properties.getMaxRetries());
                return operation.get();
            } catch (Exception e) {
                lastException = e;
                attempt++;

                if (attempt >= properties.getMaxRetries()) {
                    logger.error("Failed to execute {} after {} attempts", operationName, properties.getMaxRetries(), e);
                    break;
                }

                if (isRetryable(e)) {
                    logger.warn("Attempt {} failed for {}. Retrying in {}ms. Error: {}", 
                        attempt, operationName, waitTime, e.getMessage());
                    try {
                        Thread.sleep(waitTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.error("Retry interrupted for {}", operationName, ie);
                        throw new KinesisProducerException(
                            "Retry interrupted", "RETRY_INTERRUPTED", false, ie);
                    }
                    waitTime = (long) (waitTime * properties.getBackoffMultiplier());
                } else {
                    logger.error("Non-retryable error occurred in {}: {}", operationName, e.getMessage());
                    throw new KinesisProducerException(
                        "Non-retryable error: " + e.getMessage(), "NON_RETRYABLE", false, e);
                }
            }
        }

        throw new KinesisProducerException(
            "Failed after " + properties.getMaxRetries() + " retries: " + lastException.getMessage(),
            "MAX_RETRIES_EXCEEDED",
            false,
            lastException);
    }

    private boolean isRetryable(Exception e) {
        if (e instanceof KinesisProducerException) {
            return ((KinesisProducerException) e).isRetryable();
        }

        String message = e.getMessage() != null ? e.getMessage().toLowerCase() : "";

        return e instanceof java.io.IOException
            || e instanceof java.net.SocketTimeoutException
            || message.contains("throttl")
            || message.contains("timeout")
            || message.contains("connection")
            || message.contains("temporary");
    }
}


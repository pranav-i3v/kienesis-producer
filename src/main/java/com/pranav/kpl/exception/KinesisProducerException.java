package com.pranav.kpl.exception;

public class KinesisProducerException extends RuntimeException {
    private final String errorCode;
    private final boolean retryable;

    public KinesisProducerException(String message, String errorCode, boolean retryable) {
        super(message);
        this.errorCode = errorCode;
        this.retryable = retryable;
    }

    public KinesisProducerException(String message, String errorCode, boolean retryable, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.retryable = retryable;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public boolean isRetryable() {
        return retryable;
    }
}

package com.pranav.kpl.model;

import software.amazon.awssdk.awscore.exception.AwsServiceException;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public record FailureDetails(String code, String message) {

    public static FailureDetails from(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        if (current instanceof AwsServiceException serviceException && serviceException.awsErrorDetails() != null) {
            return new FailureDetails(serviceException.awsErrorDetails().errorCode(), serviceException.getMessage());
        }
        return new FailureDetails("KINESIS_SUBMISSION_ERROR", current.getMessage());
    }
}

# AWS Kinesis Producer Library

Reusable Spring Boot auto-configuration for publishing application records to Amazon Kinesis Data Streams through the Amazon Kinesis Producer Library (KPL).

KPL is the only data publishing path. The AWS SDK `KinesisAsyncClient` is created only for control-plane operations such as stream health checks.

## Features

- KPL-backed single and batch publishing
- Blocking and `CompletableFuture` APIs
- Structured publish results with event ID, shard ID, sequence number, attempt count, and final failure details
- Per-record batch results, including validation failures
- Retryable AWS error-code classification for failed batch records
- Exponential backoff with full jitter and capped retry delay
- Spring-managed KPL and SDK client lifecycle
- Default AWS credential-provider chain with application override support

## Installation

Add the library to the consuming application's `pom.xml`:

```xml
<dependency>
    <groupId>com.pranav</groupId>
    <artifactId>kienesis-producer</artifactId>
    <version>0.0.1</version>
</dependency>
```

## Configuration

Use the `aws.kinesis.producer.*` prefix:

```properties
aws.kinesis.producer.enabled=true
aws.kinesis.producer.region=us-east-1
aws.kinesis.producer.stream-name=

aws.kinesis.producer.max-connections=24
aws.kinesis.producer.request-timeout=60000
aws.kinesis.producer.record-max-buffered-time=100

aws.kinesis.producer.aggregation-enabled=true
aws.kinesis.producer.aggregation-max-size=51200
aws.kinesis.producer.aggregation-max-count=100

aws.kinesis.producer.max-records-per-batch=500
aws.kinesis.producer.max-record-size-bytes=1048576
aws.kinesis.producer.max-batch-records=500
aws.kinesis.producer.max-batch-payload-bytes=5242880

aws.kinesis.producer.max-retries=3
aws.kinesis.producer.retry-wait-time-ms=1000
aws.kinesis.producer.backoff-multiplier=2.0
aws.kinesis.producer.retry-max-delay-ms=30000
aws.kinesis.producer.retryable-error-codes=ProvisionedThroughputExceededException,LimitExceededException,InternalFailure,InternalFailureException,ServiceUnavailable,ServiceUnavailableException,RequestTimeout,RequestTimeoutException,KMSDisabledException,KMSInvalidStateException,KMSThrottlingException
```

`stream-name` is an optional default. If a `PutRecordRequest` contains a non-blank stream name, the request value is used. Otherwise, the configured default is used.

## Quick Start

Inject the public service:

```java
import com.pranav.kpl.service.KinesisProducerService;

@Service
public class EventService {

    private final KinesisProducerService producer;

    public EventService(KinesisProducerService producer) {
        this.producer = producer;
    }
}
```

Publish one record synchronously:

```java
import com.pranav.kpl.model.PublishResult;
import com.pranav.kpl.request.PutRecordRequest;

public PublishResult publishEvent(String userId, Map<String, Object> eventData) {
    PublishResult result = producer.putRecord(new PutRecordRequest(
            "events",
            "user-" + userId,
            eventData
    ));

    if (!result.successful()) {
        logger.warn("Kinesis publish failed: code={}, message={}, attempts={}",
                result.errorCode(), result.errorMessage(), result.attempts());
    }

    return result;
}
```

Publish one record asynchronously:

```java
producer.putRecordAsync(new PutRecordRequest("events", "user-" + userId, eventData))
        .thenAccept(result -> {
            if (result.successful()) {
                logger.info("Published to shard={}, sequence={}",
                        result.shardId(), result.sequenceNumber());
            } else {
                logger.warn("Publish failed: {}", result.errorCode());
            }
        });
```

Publish a batch:

```java
import com.pranav.kpl.model.BatchPublishResult;

BatchPublishResult result = producer.putRecordBatch(new PutRecordRequest(
        "events",
        "batch-" + System.currentTimeMillis(),
        List.of(
                Map.of("event", "created"),
                Map.of("event", "updated")
        )
));

logger.info("Published records: success={}, failed={}",
        result.successfulCount(), result.failedCount());
```

Run a stream health check:

```java
producer.healthCheck("events")
        .thenAccept(health -> logger.info("stream={}, healthy={}, status={}",
                health.streamName(), health.healthy(), health.streamStatus()));
```

## Public API

`KinesisProducerService` exposes the caller-facing operations:

| Method | Return type | Description |
|--------|-------------|-------------|
| `putRecord(PutRecordRequest request)` | `PublishResult` | Blocking single-record publish. |
| `putRecordAsync(PutRecordRequest request)` | `CompletableFuture<PublishResult>` | Async single-record publish. |
| `putRecordBatch(PutRecordRequest request)` | `BatchPublishResult` | Blocking batch publish. |
| `putRecordBatchAsync(PutRecordRequest request)` | `CompletableFuture<BatchPublishResult>` | Async batch publish. |
| `healthCheck(String streamName)` | `CompletableFuture<ProducerHealthResult>` | SDK control-plane stream health check. |

`PutRecordRequest` contains:

- `streamName`: target stream, optional when `aws.kinesis.producer.stream-name` is configured
- `partitionKey`: required partition key
- `data`: payload for single-record publishing
- `dataList`: payload list for batch publishing

`PublishResult` contains:

- `eventId`
- `successful`
- `shardId`
- `sequenceNumber`
- `attempts`
- `errorCode`
- `errorMessage`

`BatchPublishResult` contains one `PublishResult` per submitted logical record and preserves request order.

## Validation

The producer validates records before submitting them to KPL:

- Stream name must be present through the request or default configuration.
- Partition key must be non-blank.
- Payload must be non-null and non-empty.
- Serialized record size must be between 1 byte and 1 MiB.
- A submitted batch can contain at most 500 logical records.
- Total serialized logical batch payload must be at most 5 MiB.

Validation failures are returned as failed `PublishResult` values. Invalid records are not submitted to KPL.

## Retry Behavior

KPL performs its own internal publishing behavior first. After KPL returns a failed `UserRecordResult`, this library retries only failed batch records whose final AWS error code is configured as retryable.

`aws.kinesis.producer.max-retries` means additional resubmissions after the initial KPL result. For example, `max-retries=3` allows the initial KPL submission plus up to three library-level resubmissions for retryable batch failures.

Retry delay uses:

- `retry-wait-time-ms` as the initial backoff value
- `backoff-multiplier` for exponential growth
- `retry-max-delay-ms` as the cap
- full jitter, so the actual delay is randomized between zero and the capped backoff value

Non-retryable failures are returned with their final error code and message.

## Health Checks

`healthCheck(String streamName)` calls `DescribeStreamSummary` through `KinesisAsyncClient`. A stream is reported healthy only when the stream status is `ACTIVE`.

This method does not publish records and does not use KPL.

## AWS Credentials

The auto-configuration creates an `AwsCredentialsProvider` using `DefaultCredentialsProvider.create()` when the application has not already supplied one.

The default chain supports common AWS credential sources such as environment variables, system properties, local AWS profile files, ECS/EKS credentials, EC2 instance profile credentials, and IAM roles.

## Spring Beans

The auto-configuration registers:

- `AwsCredentialsProvider`
- `KinesisAsyncClient`, closed by Spring with `close`
- `KinesisProducer`, destroyed by Spring with `destroy`
- `KinesisProducerService`
- collaborators for validation, publishing, retry policy, health checks, and JSON serialization

Applications can override these beans by defining their own bean of the same type.

## Project Structure

```text
kienesis-producer/
|-- src/main/java/com/pranav/kpl/
|   |-- config/
|   |   |-- KinesisProducerAutoConfiguration.java
|   |   `-- KinesisProducerProperties.java
|   |-- service/
|   |   |-- KinesisProducerService.java
|   |   `-- KinesisProducerHealthService.java
|   |-- request/
|   |   `-- PutRecordRequest.java
|   |-- model/
|   |   |-- AttemptOutcome.java
|   |   |-- BatchPublishResult.java
|   |   |-- FailureDetails.java
|   |   |-- KinesisRecord.java
|   |   |-- PreparedBatch.java
|   |   |-- PreparedRecord.java
|   |   |-- ProducerHealthResult.java
|   |   |-- PublishResult.java
|   |   `-- TrackedRecord.java
|   |-- internal/
|   |   |-- KinesisRecordValidator.java
|   |   |-- KplRecordPublisher.java
|   |   `-- KinesisRetryPolicy.java
|   |-- util/
|   |   `-- JsonUtil.java
|   `-- exception/
|       `-- KinesisProducerException.java
|-- src/main/resources/
|   |-- META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
|   `-- application.properties
`-- pom.xml
```

## Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| Spring Boot | `4.1.1` | Auto-configuration and application context integration. |
| AWS SDK BOM | `2.27.20` | AWS SDK dependency management. |
| AWS SDK Kinesis | BOM-managed | Control-plane health checks. |
| Amazon Kinesis Producer | `1.0.7` | KPL data publishing path. |
| Jackson Databind | Spring Boot-managed | JSON serialization. |
| Jackson JSR-310 | Spring Boot-managed | Java time serialization. |

## Build

```bash
mvn clean package
```

For local development with the Maven wrapper:

```bash
./mvnw test
```

On Windows:

```powershell
.\mvnw.cmd test
```

## Troubleshooting

| Issue | Likely cause | Check |
|-------|--------------|-------|
| `INVALID_STREAM_NAME` | No stream name in request or config | Set request stream name or `aws.kinesis.producer.stream-name`. |
| `INVALID_PARTITION_KEY` | Blank partition key | Provide a stable non-blank partition key. |
| `INVALID_PAYLOAD` | Null or empty payload | Submit a non-empty payload object. |
| `INVALID_RECORD_SIZE` | Serialized record exceeds 1 MiB | Reduce payload size before publishing. |
| Failed health check | Stream missing, inactive, or access denied | Check stream name, stream status, region, and IAM permissions. |
| Retryable publish failures | Kinesis throttling or temporary service issue | Check shard throughput and retry/error-code configuration. |

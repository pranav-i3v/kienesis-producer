# AWS Kinesis Producer Library

A lightweight, reusable Spring Boot library for sending data to AWS Kinesis streams. Designed to be integrated into any microservice with batch processing, async operations, automatic retry logic, and exponential backoff.

## Features

✅ **Batch Processing** - Send up to 500 records per batch  
✅ **Async Operations** - Non-blocking `CompletableFuture` based async send  
✅ **Retry Logic** - Automatic retry with exponential backoff  
✅ **Error Handling** - Retryable vs non-retryable error classification  
✅ **JSON Serialization** - Automatic JSON conversion with Jackson  
✅ **Dynamic Stream Name** - Pass stream name per API call  
✅ **Partition Key Control** - Flexible partition key strategy  
✅ **Configurable** - All parameters via `.properties` file  
✅ **Production Ready** - Comprehensive logging and error handling  

## Installation

### Maven Dependency

Add this to your microservice's `pom.xml`:

```xml
<dependency>
    <groupId>com.pranav</groupId>
    <artifactId>kienesis-producer</artifactId>
    <version>0.0.1</version>
</dependency>
```

### Configuration

Add AWS Kinesis configuration to your `application.properties`:

```properties
# AWS Region
aws.kinesis.region=us-east-1

# Connection Settings
aws.kinesis.max-connections=24
aws.kinesis.request-timeout=60000

# Batch Settings
aws.kinesis.record-max-buffered-time=100
aws.kinesis.max-records-per-batch=500

# Thread Pool for Async Operations
aws.kinesis.thread-pool-core-size=5
aws.kinesis.thread-pool-max-size=10
aws.kinesis.thread-pool-queue-capacity=100

# Retry Configuration
aws.kinesis.max-retries=3
aws.kinesis.retry-wait-time-ms=1000
aws.kinesis.backoff-multiplier=2.0

# Logging
logging.level.com.pranav.kpl=INFO
logging.level.software.amazon.awssdk=WARN
```

**Note:** Stream name is NOT configured here. Pass it dynamically when calling the API.

## Quick Start

### 1. Inject the Service

```java
@Service
public class EventService {
    
    private final KinesisProducerService producer;
    
    public EventService(KinesisProducerService producer) {
        this.producer = producer;
    }
}
```

### 2. Send Single Record (Blocking)

```java
public void publishEvent(String event) {
    Map<String, Object> eventData = Map.of(
        "event", event,
        "timestamp", System.currentTimeMillis()
    );
    
    producer.putRecord(
        "my-kinesis-stream",      // Stream name
        "user-" + userId,          // Partition key
        eventData                  // Data payload
    );
}
```

### 3. Send Single Record (Async)

```java
public void publishEventAsync(String event) {
    producer.putRecordAsync(
        "my-kinesis-stream",
        "user-" + userId,
        eventData
    )
    .thenAccept(v -> logger.info("Event sent"))
    .exceptionally(ex -> {
        logger.error("Failed to send event", ex);
        return null;
    });
}
```

### 4. Send Batch Records (Blocking)

```java
public void publishBatch(List<Map<String, Object>> events) {
    List<KinesisRecord> records = events.stream()
        .map(event -> new KinesisRecord(
            null,                              // record ID (auto-generated)
            "batch-" + System.currentTimeMillis(),  // partition key
            event                              // data
        ))
        .toList();
    
    producer.putRecordBatch("my-kinesis-stream", records);
}
```

### 5. Send Batch Records (Async)

```java
public void publishBatchAsync(List<Map<String, Object>> events) {
    List<KinesisRecord> records = buildRecords(events);
    
    producer.putRecordBatchAsync("my-kinesis-stream", records)
        .thenAccept(v -> logger.info("Batch sent"))
        .exceptionally(ex -> {
            logger.error("Batch failed", ex);
            return null;
        });
}
```

## API Reference

### KinesisProducerService

#### putRecord(String streamName, String partitionKey, Object data)

Send a single record to Kinesis stream (blocking).

**Parameters:**
- `streamName` - Kinesis stream name
- `partitionKey` - Partition key for record distribution
- `data` - Record payload (will be JSON serialized)

**Example:**
```java
producer.putRecord("events", "user-123", 
    Map.of("action", "login", "ip", "192.168.1.1"));
```

#### putRecordAsync(String streamName, String partitionKey, Object data)

Send a single record to Kinesis stream (async, non-blocking).

**Returns:** `CompletableFuture<Void>`

**Example:**
```java
producer.putRecordAsync("events", "user-123", data)
    .thenApply(v -> "sent")
    .exceptionally(ex -> "error");
```

#### putRecordBatch(String streamName, List<KinesisRecord> records)

Send multiple records to Kinesis stream in a batch (blocking).

**Parameters:**
- `streamName` - Kinesis stream name
- `records` - List of KinesisRecord objects

**Example:**
```java
List<KinesisRecord> records = List.of(
    new KinesisRecord(null, "key1", Map.of("id", 1)),
    new KinesisRecord(null, "key2", Map.of("id", 2))
);
producer.putRecordBatch("events", records);
```

#### putRecordBatchAsync(String streamName, List<KinesisRecord> records)

Send multiple records to Kinesis stream in a batch (async, non-blocking).

**Returns:** `CompletableFuture<Void>`

**Example:**
```java
producer.putRecordBatchAsync("events", records)
    .thenAccept(v -> logger.info("Batch sent"));
```

## Data Models

### KinesisRecord

Represents a record to be sent to Kinesis.

```java
public class KinesisRecord {
    private String recordId;           // Optional, auto-generated
    private String partitionKey;       // Required
    private Object data;               // Required, will be JSON serialized
    private LocalDateTime timestamp;   // Auto-set to current time
    private String sequenceNumber;     // Set by Kinesis on send
    private String shardId;            // Set by Kinesis on send
}
```

**Usage:**
```java
KinesisRecord record = new KinesisRecord(
    "record-1",                    // recordId
    "user-123",                    // partitionKey
    Map.of("event", "purchase")    // data
);
```

## Configuration Options

| Property | Default | Description |
|----------|---------|-------------|
| `aws.kinesis.region` | us-east-1 | AWS region |
| `aws.kinesis.max-connections` | 24 | HTTP connection pool size |
| `aws.kinesis.request-timeout` | 60000 | Request timeout in milliseconds |
| `aws.kinesis.record-max-buffered-time` | 100 | Buffer time before sending in ms |
| `aws.kinesis.max-records-per-batch` | 500 | Maximum records per batch |
| `aws.kinesis.aggregation-enabled` | true | Enable record aggregation |
| `aws.kinesis.aggregation-max-size` | 51200 | Max aggregation size in bytes |
| `aws.kinesis.thread-pool-core-size` | 5 | Async executor core threads |
| `aws.kinesis.thread-pool-max-size` | 10 | Async executor max threads |
| `aws.kinesis.thread-pool-queue-capacity` | 100 | Async task queue capacity |
| `aws.kinesis.max-retries` | 3 | Maximum retry attempts |
| `aws.kinesis.retry-wait-time-ms` | 1000 | Initial retry wait time in ms |
| `aws.kinesis.backoff-multiplier` | 2.0 | Exponential backoff multiplier |

## Retry Logic

The library automatically retries failed requests with exponential backoff:

```
Attempt 1: Immediate
    ↓ (fails, retryable)
Attempt 2: Wait 1000ms
    ↓ (fails, retryable)
Attempt 3: Wait 2000ms
    ↓ (fails, retryable)
Attempt 4: Wait 4000ms
    ↓ (fails)
FAILURE
```

Retryable errors include:
- Throttling errors
- Connection timeouts
- I/O exceptions
- Temporary network failures

Non-retryable errors:
- Stream not found
- Invalid record format
- Access denied (IAM)

## Error Handling

### KinesisProducerException

Custom exception thrown by the library.

```java
public class KinesisProducerException extends RuntimeException {
    private String errorCode;      // Machine-readable error code
    private boolean retryable;     // Whether error can be retried
}
```

**Usage:**
```java
try {
    producer.putRecord(stream, key, data);
} catch (KinesisProducerException e) {
    if (e.isRetryable()) {
        logger.warn("Retryable error: {}", e.getErrorCode());
    } else {
        logger.error("Non-retryable error: {}", e.getErrorCode());
    }
}
```

## Example: Complete Service Implementation

```java
@Service
@Slf4j
public class OrderEventService {
    
    private final KinesisProducerService producer;
    
    public OrderEventService(KinesisProducerService producer) {
        this.producer = producer;
    }
    
    public void publishOrderCreated(Order order) {
        Map<String, Object> event = Map.of(
            "orderId", order.getId(),
            "userId", order.getUserId(),
            "amount", order.getAmount(),
            "timestamp", System.currentTimeMillis()
        );
        
        try {
            producer.putRecord(
                "order-events",
                "user-" + order.getUserId(),
                event
            );
            log.info("Order created event published: {}", order.getId());
        } catch (KinesisProducerException e) {
            log.error("Failed to publish order event", e);
            throw e;
        }
    }
    
    public void publishOrdersAsync(List<Order> orders) {
        List<KinesisRecord> records = orders.stream()
            .map(order -> new KinesisRecord(
                order.getId(),
                "user-" + order.getUserId(),
                Map.of(
                    "orderId", order.getId(),
                    "status", "created"
                )
            ))
            .toList();
        
        producer.putRecordBatchAsync("order-events", records)
            .thenAccept(v -> log.info("Batch of {} orders published", records.size()))
            .exceptionally(ex -> {
                log.error("Failed to publish orders batch", ex);
                return null;
            });
    }
}
```

## Configuration Profiles

### Development Configuration

```properties
# dev-application.properties
aws.kinesis.region=us-east-1
aws.kinesis.max-connections=10
aws.kinesis.max-records-per-batch=100
aws.kinesis.thread-pool-core-size=2
aws.kinesis.thread-pool-max-size=5
aws.kinesis.thread-pool-queue-capacity=50
logging.level.com.pranav.kpl=DEBUG
```

### Production Configuration

```properties
# prod-application.properties
aws.kinesis.region=us-east-1
aws.kinesis.max-connections=32
aws.kinesis.max-records-per-batch=500
aws.kinesis.thread-pool-core-size=10
aws.kinesis.thread-pool-max-size=20
aws.kinesis.thread-pool-queue-capacity=500
logging.level.com.pranav.kpl=INFO
```

## Performance Tips

1. **Use Batch Operations** - For high throughput, batch multiple records
2. **Use Async Operations** - For low latency, use async methods
3. **Optimize Partition Key** - Distribute across shards using user ID, timestamp, etc.
4. **Configure Thread Pool** - Adjust pool size based on workload
5. **Monitor Queue Size** - Watch for queue overflow in high-load scenarios

## AWS Credentials

The library uses AWS SDK v2 default credential provider chain:

1. **IAM Roles** (recommended for production)
   ```bash
   # No configuration needed on EC2/ECS with IAM role attached
   ```

2. **Environment Variables**
   ```bash
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   export AWS_REGION=us-east-1
   ```

3. **Credentials File** (local development)
   ```bash
   ~/.aws/credentials
   ```

## Logging

Configure logging in `application.properties`:

```properties
# Debug library operations
logging.level.com.pranav.kpl=DEBUG

# Reduce AWS SDK verbosity
logging.level.software.amazon.awssdk=WARN

# Log to file
logging.file.name=logs/kinesis.log
logging.file.max-size=10MB
logging.file.max-history=10
```

**Log Examples:**
```
2026-09-01 10:00:00 - Kinesis client created for region: us-east-1
2026-09-01 10:00:05 - Putting record to stream: my-stream, partition key: user-123
2026-09-01 10:00:05 - Record sent successfully. Sequence: 49601..., Shard: shardId-000...
```

## Building & Testing

### Build
```bash
mvn clean install
```

### Run Tests
```bash
mvn test
```

### Package
```bash
mvn clean package
```

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| Stream not found | Wrong stream name | Verify stream name is correct and exists in AWS |
| Access denied | Missing IAM permissions | Check IAM role/credentials has kinesis:PutRecord access |
| Timeout errors | Network issues | Increase `request-timeout` in configuration |
| Queue overflow | Too many async tasks | Reduce batch size or increase thread pool size |
| High memory usage | Large batch sizes | Reduce `max-records-per-batch` |
| Slow throughput | Limited connections | Increase `max-connections` |

## Project Structure

```
kienesis-producer/
├── src/
│   ├── main/
│   │   ├── java/com/pranav/kpl/
│   │   │   ├── config/
│   │   │   │   ├── KinesisConfig.java
│   │   │   │   └── AwsClientConfig.java
│   │   │   ├── service/
│   │   │   │   ├── KinesisProducerService.java
│   │   │   │   └── RetryService.java
│   │   │   ├── model/
│   │   │   │   ├── KinesisRecord.java
│   │   │   │   └── ProducerResponse.java
│   │   │   ├── exception/
│   │   │   │   └── KinesisProducerException.java
│   │   │   ├── util/
│   │   │   │   └── JsonUtil.java
│   │   │   └── KienesisProducerApplication.java
│   │   └── resources/
│   │       └── application.properties
│   └── test/
│       └── java/com/pranav/kpl/
│           └── service/
│               └── KinesisProducerServiceTest.java
├── pom.xml
├── README.md
├── example-application.properties
├── production-application.properties
└── dev-application.properties
```

## Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| Spring Boot | 4.1.1 | Framework |
| AWS Kinesis SDK | 2.25.0 | AWS client |
| AWS KPL | 0.15.11 | Batch processing |
| Jackson | Latest | JSON serialization |
| Lombok | Latest | Boilerplate reduction |

## Support & Contributing

For issues, feature requests, or contributions, please refer to project documentation.

## License

This project is open source and available under the MIT License.

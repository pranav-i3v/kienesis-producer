package com.pranav.kpl.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pranav.kpl.util.JsonUtil;
import com.pranav.kpl.service.KinesisProducerHealthService;
import com.pranav.kpl.internal.KinesisRecordValidator;
import com.pranav.kpl.internal.KinesisRetryPolicy;
import com.pranav.kpl.internal.KplRecordPublisher;
import com.pranav.kpl.service.KinesisProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.producer.IKinesisProducer;
import software.amazon.kinesis.producer.KinesisProducer;
import software.amazon.kinesis.producer.KinesisProducerConfiguration;

@AutoConfiguration
@ConditionalOnProperty(prefix = "aws.kinesis.producer", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(KinesisProducerProperties.class)
public class KinesisProducerAutoConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(KinesisProducerAutoConfiguration.class);

    /**
     * Provides the AWS credentials used by the Kinesis producer and the
     * Kinesis control-plane client.
     *
     * <p>DefaultCredentialsProvider automatically discovers credentials from
     * the standard AWS credential provider chain, such as IAM roles, environment
     * variables, AWS profiles, and container/instance credentials.</p>
     *
     * <p>The bean is created only when the application has not provided its own
     * {@link AwsCredentialsProvider}, allowing applications to override the
     * default credential strategy when required.</p>
     */
    @Bean
    @ConditionalOnMissingBean
    public AwsCredentialsProvider awsCredentialsProvider() {
        return DefaultCredentialsProvider.create();
    }

    /**
     * Creates the asynchronous AWS Kinesis client used for Kinesis
     * control-plane operations.
     *
     * <p>This client is separate from {@link KinesisProducer}. The KPL producer
     * is responsible for sending application records to Kinesis, while this
     * client can be used by the library for operations such as retrieving
     * stream/shard metadata, describing streams, or other Kinesis API calls
     * that are not handled directly by the KPL.</p>
     *
     * <p>The client reuses the common AWS credentials provider and region
     * configured for the producer library.</p>
     *
     * <p>Spring automatically closes the client when the application context
     * shuts down.</p>
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public KinesisAsyncClient kinesisAsyncClient(KinesisProducerProperties properties,
                                                  AwsCredentialsProvider credentialsProvider) {

        KinesisAsyncClient client = KinesisAsyncClient.builder()
                .region(Region.of(properties.getRegion()))
                .credentialsProvider(credentialsProvider)
                .build();

        logger.info("Kinesis async control client created for region: {}", properties.getRegion());
        return client;
    }

    /**
     * Creates and configures the Kinesis Producer Library (KPL) producer.
     *
     * <p>This is the main producer component responsible for accepting
     * application records and delivering them to the configured Kinesis stream.
     * KPL handles producer-side buffering, batching/aggregation, retries,
     * concurrency, and communication with Kinesis.</p>
     *
     * <p>The producer configuration is built from the library's Spring Boot
     * properties and uses the shared AWS credentials provider.</p>
     *
     * <p>The bean is created only when the application has not supplied its own
     * {@link IKinesisProducer}, allowing the application to override the
     * default producer implementation.</p>
     *
     * <p>Spring invokes {@code destroy()} when the application shuts down so
     * that the KPL producer can flush pending records and release its resources.</p>
     */
    @Bean(destroyMethod = "destroy")
    @ConditionalOnMissingBean(IKinesisProducer.class)
    public KinesisProducer kinesisProducer(KinesisProducerProperties properties,
                                           AwsCredentialsProvider credentialsProvider) {

        KinesisProducerConfiguration configuration = new KinesisProducerConfiguration()
                .setRegion(properties.getRegion())
                .setMaxConnections(properties.getMaxConnections())
                .setRequestTimeout(properties.getRequestTimeout())
                .setRecordMaxBufferedTime(properties.getRecordMaxBufferedTime())
                .setAggregationEnabled(properties.isAggregationEnabled())
                .setAggregationMaxSize(properties.getAggregationMaxSize())
                .setAggregationMaxCount(properties.getAggregationMaxCount())
                .setCredentialsProvider(credentialsProvider);

        return new KinesisProducer(configuration);
    }

    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        return new ObjectMapper().findAndRegisterModules();
    }

    @Bean
    @ConditionalOnMissingBean
    public JsonUtil jsonUtil(ObjectMapper objectMapper) {
        return new JsonUtil(objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public KinesisRecordValidator kinesisRecordValidator(JsonUtil jsonUtil, KinesisProducerProperties properties) {
        return new KinesisRecordValidator(jsonUtil, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public KplRecordPublisher kplRecordPublisher(IKinesisProducer kinesisProducer) {
        return new KplRecordPublisher(kinesisProducer);
    }

    @Bean
    @ConditionalOnMissingBean
    public KinesisRetryPolicy kinesisRetryPolicy(KinesisProducerProperties properties) {
        return new KinesisRetryPolicy(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public KinesisProducerHealthService kinesisProducerHealthService(KinesisAsyncClient kinesisAsyncClient) {
        return new KinesisProducerHealthService(kinesisAsyncClient);
    }

    @Bean
    @ConditionalOnMissingBean
    public KinesisProducerService kinesisProducerService(
            KinesisRecordValidator recordValidator,
            KplRecordPublisher recordPublisher,
            KinesisRetryPolicy retryPolicy,
            KinesisProducerHealthService healthService) {

        return new KinesisProducerService(recordValidator, recordPublisher, retryPolicy, healthService);
    }

}

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
     * Creates the asynchronous AWS Kinesis client used for record publishing
     * and control-plane operations.
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

        logger.info("Kinesis async client created for region: {}", properties.getRegion());
        return client;
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
    public KplRecordPublisher kplRecordPublisher(KinesisAsyncClient kinesisAsyncClient) {
        return new KplRecordPublisher(kinesisAsyncClient);
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

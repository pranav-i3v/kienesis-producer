package com.pranav.kpl.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pranav.kpl.service.RetryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

@Configuration
public class AwsClientConfig {
    private static final Logger logger = LoggerFactory.getLogger(AwsClientConfig.class);

    @Bean
    public KinesisClient kinesisClient(KinesisConfig.KinesisProperties properties) {
        try {
            KinesisClient client = KinesisClient.builder()
                .region(Region.of(properties.getRegion()))
                .build();
            
            logger.info("Kinesis client created for region: {}", properties.getRegion());
            return client;
        } catch (Exception e) {
            logger.error("Error creating Kinesis client", e);
            throw new RuntimeException("Failed to initialize Kinesis client", e);
        }
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public RetryService retryService(KinesisConfig.KinesisProperties properties) {
        return new RetryService(properties);
    }
}


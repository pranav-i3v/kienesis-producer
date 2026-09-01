package com.pranav.kpl.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
@EnableConfigurationProperties(KinesisConfig.KinesisProperties.class)
public class KinesisConfig {

    private static final Logger logger = LoggerFactory.getLogger(KinesisConfig.class);

    @Bean(name = "kinesisExecutor")
    public Executor kinesisExecutor(KinesisProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(properties.getThreadPoolCoreSize());
        executor.setMaxPoolSize(properties.getThreadPoolMaxSize());
        executor.setQueueCapacity(properties.getThreadPoolQueueCapacity());
        executor.setThreadNamePrefix("kinesis-producer-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();
        
        logger.info("Kinesis async executor configured - core: {}, max: {}, queue: {}", 
            properties.getThreadPoolCoreSize(), 
            properties.getThreadPoolMaxSize(),
            properties.getThreadPoolQueueCapacity());
        
        return executor;
    }

    @ConfigurationProperties(prefix = "aws.kinesis")
    public static class KinesisProperties {
        private String region = "us-east-1";
        private String streamName;
        private int maxConnections = 24;
        private long requestTimeout = 60000;
        private long recordMaxBufferedTime = 100;
        private int maxRecordsPerBatch = 500;
        private boolean aggregationEnabled = true;
        private int aggregationMaxSize = 51200;
        private int aggregationMaxCount = 100;
        private int threadPoolCoreSize = 5;
        private int threadPoolMaxSize = 10;
        private int threadPoolQueueCapacity = 100;
        private int maxRetries = 3;
        private long retryWaitTimeMs = 1000;
        private double backoffMultiplier = 2.0;

        public String getRegion() {
            return region;
        }

        public void setRegion(String region) {
            this.region = region;
        }

        public String getStreamName() {
            return streamName;
        }

        public void setStreamName(String streamName) {
            this.streamName = streamName;
        }

        public int getMaxConnections() {
            return maxConnections;
        }

        public void setMaxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
        }

        public long getRequestTimeout() {
            return requestTimeout;
        }

        public void setRequestTimeout(long requestTimeout) {
            this.requestTimeout = requestTimeout;
        }

        public long getRecordMaxBufferedTime() {
            return recordMaxBufferedTime;
        }

        public void setRecordMaxBufferedTime(long recordMaxBufferedTime) {
            this.recordMaxBufferedTime = recordMaxBufferedTime;
        }

        public int getMaxRecordsPerBatch() {
            return maxRecordsPerBatch;
        }

        public void setMaxRecordsPerBatch(int maxRecordsPerBatch) {
            this.maxRecordsPerBatch = maxRecordsPerBatch;
        }

        public boolean isAggregationEnabled() {
            return aggregationEnabled;
        }

        public void setAggregationEnabled(boolean aggregationEnabled) {
            this.aggregationEnabled = aggregationEnabled;
        }

        public int getAggregationMaxSize() {
            return aggregationMaxSize;
        }

        public void setAggregationMaxSize(int aggregationMaxSize) {
            this.aggregationMaxSize = aggregationMaxSize;
        }

        public int getAggregationMaxCount() {
            return aggregationMaxCount;
        }

        public void setAggregationMaxCount(int aggregationMaxCount) {
            this.aggregationMaxCount = aggregationMaxCount;
        }

        public int getThreadPoolCoreSize() {
            return threadPoolCoreSize;
        }

        public void setThreadPoolCoreSize(int threadPoolCoreSize) {
            this.threadPoolCoreSize = threadPoolCoreSize;
        }

        public int getThreadPoolMaxSize() {
            return threadPoolMaxSize;
        }

        public void setThreadPoolMaxSize(int threadPoolMaxSize) {
            this.threadPoolMaxSize = threadPoolMaxSize;
        }

        public int getThreadPoolQueueCapacity() {
            return threadPoolQueueCapacity;
        }

        public void setThreadPoolQueueCapacity(int threadPoolQueueCapacity) {
            this.threadPoolQueueCapacity = threadPoolQueueCapacity;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public long getRetryWaitTimeMs() {
            return retryWaitTimeMs;
        }

        public void setRetryWaitTimeMs(long retryWaitTimeMs) {
            this.retryWaitTimeMs = retryWaitTimeMs;
        }

        public double getBackoffMultiplier() {
            return backoffMultiplier;
        }

        public void setBackoffMultiplier(double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
        }
    }
}

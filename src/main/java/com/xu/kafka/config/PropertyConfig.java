package com.xu.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @author CharleyXu Created on 2018/8/28.
 */
@Configuration
public class PropertyConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String broker;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    @Value("${spring.kafka.consumer.enable-auto-commit}")
    private String enableAutoCommit;

    @Value("${spring.kafka.listener.concurrency}")
    private Integer concurrency;

    @Value("${spring.kafka.consumer.max-poll-records}")
    private Integer maxPollRecords;
    @Value("${spring.kafka.consumer.fetch-max-wait}")
    private Integer fetchMaxWait;

    public String getBroker() {
        return broker;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public String getEnableAutoCommit() {
        return enableAutoCommit;
    }

    public Integer getConcurrency() {
        return concurrency;
    }

    public Integer getMaxPollRecords() {
        return maxPollRecords;
    }

    public Integer getFetchMaxWait() {
        return fetchMaxWait;
    }
}

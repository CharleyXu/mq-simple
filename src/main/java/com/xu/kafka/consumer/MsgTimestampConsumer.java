package com.xu.kafka.consumer;

import com.xu.kafka.config.KafkaConsumerConfig;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author CharleyXu Created on 2018/8/29.
 *
 * 基于时间戳，获取offsets和消费
 */
@Component
public class MsgTimestampConsumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private KafkaConsumerConfig kafkaConsumerConfig;

    /**
     * 指定开始时间与结束时间获取offsets
     */
    public void getOffsetsForTimes(String topic, String start, String end) {
        KafkaConsumer<String, String> kafkaConsumer = getConsumer();

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        long startTime = LocalDateTime.parse(start, dateTimeFormatter)
                .toInstant(ZoneOffset.of("+8"))
                .toEpochMilli();
        long endTime = LocalDateTime.parse(end, dateTimeFormatter)
                .toInstant(ZoneOffset.of("+8"))
                .toEpochMilli();
        TopicPartition topicPartition0 = new TopicPartition(topic, 0);
        Map<TopicPartition, Long> startOffsetsMap = new HashMap<>();
        startOffsetsMap.put(topicPartition0, startTime);
        Map<TopicPartition, OffsetAndTimestamp> startPartitionOffsetsMap = kafkaConsumer
                .offsetsForTimes(startOffsetsMap);

        Map<TopicPartition, Long> endOffsetsMap = new HashMap<>();
        endOffsetsMap.put(topicPartition0, endTime);
        Map<TopicPartition, OffsetAndTimestamp> endPartitionOffsetsMap = kafkaConsumer
                .offsetsForTimes(endOffsetsMap);

        long partition0StartOffset = 0;
        if (startOffsetsMap.get(topicPartition0) != null) {
            partition0StartOffset = startPartitionOffsetsMap.get(topicPartition0).offset();
        }
        long partition0EndOffset = 0;
        if (endPartitionOffsetsMap.get(topicPartition0) != null) {
            partition0EndOffset = endPartitionOffsetsMap.get(topicPartition0).offset();
        } else {
            if (partition0StartOffset > 0) {
                partition0EndOffset = kafkaConsumer.endOffsets(Arrays.asList(topicPartition0))
                        .get(topicPartition0);
            }
        }
        long total = partition0EndOffset - partition0StartOffset;
        logger.info("partition0StartOffset:{},partition0EndOffset:{},total:{}",
                partition0StartOffset, partition0EndOffset, total);

    }

    /**
     * 基于时间戳查询消息,从某个时间点的offset开始消费
     */
    public void consumeForTimes(String topic, String start) {
        KafkaConsumer<String, String> kafkaConsumer = getConsumer();

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        long startTime = LocalDateTime.parse(start, dateTimeFormatter)
                .toInstant(ZoneOffset.of("+8"))
                .toEpochMilli();
        TopicPartition topicPartition0 = new TopicPartition(topic, 0);
        kafkaConsumer.assign(Collections.singletonList(topicPartition0));
        Map<TopicPartition, Long> startOffsetsMap = new HashMap<>();
        startOffsetsMap.put(topicPartition0, startTime);
        Map<TopicPartition, OffsetAndTimestamp> startPartitionOffsetsMap = kafkaConsumer
                .offsetsForTimes(startOffsetsMap);
        long partition0StartOffset = 0;
        if (startOffsetsMap.get(topicPartition0) != null) {
            partition0StartOffset = startPartitionOffsetsMap.get(topicPartition0).offset();
        }

        logger.info("设置各分区初始偏移量:{}", partition0StartOffset);
        kafkaConsumer.seek(topicPartition0, partition0StartOffset);
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(50);
        for (ConsumerRecord<?, ?> record : consumerRecords) {
            Optional<?> kafkaMessage = Optional.ofNullable(record.value());
            if (kafkaMessage.isPresent()) {
                Object message = record.value();
                long timestamp = record.timestamp();
                long offset = record.offset();
                logger.info("received message={},timestamp={},topic={}", message,
                        timestamp, offset);
            }
        }
    }

    public KafkaConsumer<String, String> getConsumer() {
        Map<String, Object> consumerConfigs = kafkaConsumerConfig.consumerConfigs();
        return new KafkaConsumer<>(consumerConfigs);
    }
}

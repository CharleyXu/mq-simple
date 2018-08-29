package com.xu.kafka.consumer;

import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;

/**
 * @author CharleyXu Created on 2018/8/28.
 *
 * KafkaListener设置批量消费
 */
public class MyKafkaListener {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    private static final String TOPIC = "topic-1";

    @KafkaListener(id = "id0", topicPartitions = {
            @TopicPartition(topic = TOPIC, partitions = {"0"})})
    public void listenPartition0(List<ConsumerRecord<?, ?>> records) {
        LOGGER.info("Id0 Listener, Thread ID: " + Thread.currentThread().getId());
        LOGGER.info("Id0 Records Size " + records.size());

        for (ConsumerRecord<?, ?> record : records) {
            Optional<?> kafkaMessage = Optional.ofNullable(record.value());
            LOGGER.info("Received: " + record);
            if (kafkaMessage.isPresent()) {
                Object message = record.value();
                String topic = record.topic();
                LOGGER.info("p0 Received topic={} message={}", topic, message);
            }
        }
    }

    @KafkaListener(id = "id1", topicPartitions = {
            @TopicPartition(topic = TOPIC, partitions = {"1"})})
    public void listenPartition1(List<ConsumerRecord<?, ?>> records) {
        LOGGER.info("Id1 Listener, Thread ID: " + Thread.currentThread().getId());
        LOGGER.info("Id1 Records Size " + records.size());

        for (ConsumerRecord<?, ?> record : records) {
            Optional<?> kafkaMessage = Optional.ofNullable(record.value());
            LOGGER.info("Received: " + record);
            if (kafkaMessage.isPresent()) {
                Object message = record.value();
                String topic = record.topic();
                LOGGER.info("p1 Received topic={} message={}", topic, message);
            }
        }
    }


}

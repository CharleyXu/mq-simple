package com.xu.kafka.example;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by CharleyXu on 2020-07-30
 */
@Slf4j
public class KafkaConsumerProducerDemo {

    public static void main(String[] args) throws InterruptedException, TimeoutException {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        CountDownLatch latch = new CountDownLatch(2);
        Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync, null, false, 10000, -1, latch);
        producerThread.start();

        Consumer consumerThread = new Consumer(KafkaProperties.TOPIC, "DemoConsumer", Optional.empty(), false, 10000, latch);
        consumerThread.start();

        if (!latch.await(5, TimeUnit.MINUTES)) {
            throw new TimeoutException("Timeout after 5 minutes waiting for demo producer and consumer to finish");
        }
        consumerThread.shutdown();
        log.info("All finished!");
    }

}

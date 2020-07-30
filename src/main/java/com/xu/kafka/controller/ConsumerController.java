package com.xu.kafka.controller;

import com.xu.kafka.consumer.MsgTimestampConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author CharleyXu Created on 2018/8/29.
 */
@RestController
public class ConsumerController {

    @Autowired
    private MsgTimestampConsumer timestampConsumer;

    @PostMapping(value = "/getOffsets", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public String getOffsetsForTimes(
            @RequestParam(defaultValue = "topic-1") String topic, String start,
            String end) {
        timestampConsumer.getOffsetsForTimes(topic, start, end);
        return "OK";
    }

    @PostMapping(value = "/consumeForTimes", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public String consumeForTimes(
            @RequestParam(defaultValue = "topic-1") String topic, String start) {
        timestampConsumer.consumeForTimes(topic, start);
        return "OK";
    }

}

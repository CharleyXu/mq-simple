package com.xu.kafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.xu.kafka.entity.Message;
import com.xu.kafka.producer.MsgProducer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author CharleyXu Created on 2018/8/28.
 */
@RestController
public class ProducerController {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private MsgProducer msgProducer;

    @PostMapping(value = "/send", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public String sendMsg(@RequestParam(value = "topic", defaultValue = "topic-1") String topic,
            @RequestParam(value = "key", required = false) String key,
            @RequestParam String content) {
        logger.info("sendMsg, topic={},key={}, content={}", topic, key, content);
        ObjectMapper objectMapper = new ObjectMapper();
        Message message = new Message(System.currentTimeMillis(), content,
                LocalDateTime.now(ZoneId.systemDefault()));
        String value = null;
        try {
            value = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
        }
        if (Strings.isNullOrEmpty(key)) {
            msgProducer.sendMsg(topic, value);
        } else {
            msgProducer.sendMsg(topic, key, value);
        }
        return value;
    }

}

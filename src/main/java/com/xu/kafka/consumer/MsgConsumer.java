package com.xu.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author CharleyXu Created on 2018/8/28.
 *
 * 消费者
 */
@Component
public class MsgConsumer {

    private Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @KafkaListener(topics = {"topic-1", "topic-2"})
    public void processMsg(String msg) {
        LOGGER.info("{} is consumed", msg);
    }
    
}

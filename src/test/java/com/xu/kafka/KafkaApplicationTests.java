package com.xu.kafka;

import com.xu.kafka.producer.MsgProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaApplicationTests {

    @Autowired
    private MsgProducer msgProducer;

    @Test
    public void contextLoads() {
        msgProducer.sendMsg("topic-1", "id0", "topic--------1");
        msgProducer.sendMsg("topic-2", "id1", "topic--------2");
    }

}

package com.xu.kafka.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.LocalDateTime;
import org.springframework.format.annotation.DateTimeFormat;

/**
 * @author CharleyXu Created on 2018/8/28.
 */
public class Message {

    public Message(Long id, String msg, LocalDateTime timestamp) {
        this.id = id;
        this.msg = msg;
        this.timestamp = timestamp;
    }

    //Id
    private Long id;
    //消息
    private String msg;
    //时间戳
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime timestamp;

    public Long getId() {
        return id;
    }

    public String getMsg() {
        return msg;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id=" + id +
                ", msg='" + msg + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}

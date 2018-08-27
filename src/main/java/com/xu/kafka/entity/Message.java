package com.xu.kafka.entity;

/**
 * @author CharleyXu Created on 2018/8/28.
 */
public class Message {

  //Id
  private Long id;
  //消息
  private String msg;
  //时间戳
  private Long timestamp;

  public Message(Long id, String msg, Long timestamp) {
    this.id = id;
    this.msg = msg;
    this.timestamp = timestamp;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getMsg() {
    return msg;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }
}

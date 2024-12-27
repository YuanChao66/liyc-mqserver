package com.liyc.mqs.mqserver.core;

import java.io.Serializable;
import java.util.UUID;

/**
 * 消息基础类
 *
 * @author Liyc
 * @date 2024/12/12 14:51
 **/

public class Message implements Serializable {
    //基础属性
    private BasicProperties basicProperties;
    //消息体
    private byte[] body;
    //消息存储起始位
    private long offsetBeg;
    //消息存储截止位
    private long offsetEnd;
    // isVailid 表示该消息在文件中是否有效（0x1 表示有效；0x0表示无效.类似于逻辑删除）
    private byte isValid = 0x1;;

    // 创建一个工厂方法, 让工厂方法帮我们封装一下创建 Message 对象的过程.
    // 这个方法中创建的 Message 对象, 会自动生成唯一的 MessageId
    // 万一 routingKey 和 basicProperties 里的 routingKey 冲突, 以外面的为主.
    public static Message createMessageWithID(String routingKey, BasicProperties basicProperties, byte[] body) {
        Message message = new Message();
        if (basicProperties != null) {
            message.setBasicProperties(basicProperties);
        }
        message.setMessageId("M-" + UUID.randomUUID());
        message.setBody(body);
        message.setRoutingKey(routingKey);
        return message;
    }

    public String getMessageId() {
        return basicProperties.getMessageId();
    }

    public void setMessageId(String messageId) {
            basicProperties.setMessageId(messageId);
    }

    public String getRoutingKey() {
        return basicProperties.getRoutingKey();
    }

    public void setRoutingKey(String routingKey) {
            basicProperties.setRoutingKey(routingKey);
    }

    public int getDurable() {
        return basicProperties.getDurable();
    }

    public void setDurable(int mode) {
            basicProperties.setDurable(mode);
    }

    public BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public long getOffsetBeg() {
        return offsetBeg;
    }

    public void setOffsetBeg(long offsetBeg) {
        this.offsetBeg = offsetBeg;
    }

    public long getOffsetEnd() {
        return offsetEnd;
    }

    public void setOffsetEnd(long offsetEnd) {
        this.offsetEnd = offsetEnd;
    }

    public byte getIsValid() {
        return isValid;
    }

    public void setIsValid(byte isValid) {
        this.isValid = isValid;
    }
}

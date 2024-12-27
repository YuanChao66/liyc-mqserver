package com.liyc.mqs.mqserver.core;

import java.io.Serializable;

/**
 * 消息基础属性类
 *
 * @author Liyc
 * @date 2024/12/12 15:04
 **/

public class BasicProperties implements Serializable {
    //消息Id
    private String messageId;
    //路由键
    private String routingKey;
    //是否持久化
    private int durable;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public int getDurable() {
        return durable;
    }

    public void setDurable(int durable) {
        this.durable = durable;
    }
}

package com.liyc.mqs.common;

import com.liyc.mqs.mqserver.core.BasicProperties;

import java.io.Serializable;

/**
 * 针对每个 VirtualHost 提供的⽅法, 都需要有⼀个类表⽰对应的参数
 *
 * @author Liyc
 * @date 2024/12/25 18:00
 **/

public class SubScribeReturns extends BasicReturns implements Serializable {
    private String consumerTag;
    private BasicProperties basicProperties;
    private byte[] body;

    public String getConsumerTag() {
        return consumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
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
}

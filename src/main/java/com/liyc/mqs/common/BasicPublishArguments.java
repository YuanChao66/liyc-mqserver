package com.liyc.mqs.common;

import com.liyc.mqs.mqserver.core.BasicProperties;

import java.io.Serializable;

/**
 * 针对每个 VirtualHost 提供的⽅法, 都需要有⼀个类表⽰对应的参数
 *
 * @author Liyc
 * @date 2024/12/25 18:00
 **/

public class BasicPublishArguments extends BasicArguments implements Serializable {
    private String exchangeName;
    private String routingKey;
    private BasicProperties basicProperties;
    private byte[] bytes;

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }
}

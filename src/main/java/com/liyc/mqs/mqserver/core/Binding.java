package com.liyc.mqs.mqserver.core;

/**
 * 绑定基础类
 *
 * @author Liyc
 * @date 2024/12/12 10:56
 **/

public class Binding {
    //交换机名称
    private String exchangeName;
    //队列名称
    private String queueName;
    //绑定键
    private String bindingKey;

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getBindingKey() {
        return bindingKey;
    }

    public void setBindingKey(String bindingKey) {
        this.bindingKey = bindingKey;
    }
}

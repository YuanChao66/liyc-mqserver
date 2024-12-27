package com.liyc.mqs.common;

import java.io.Serializable;

/**
 * 针对每个 VirtualHost 提供的⽅法, 都需要有⼀个类表⽰对应的参数
 *
 * @author Liyc
 * @date 2024/12/25 17:59
 **/

public class QueueBindArguments extends BasicArguments implements Serializable {

    private String exchangeName;

    private String queueName;

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

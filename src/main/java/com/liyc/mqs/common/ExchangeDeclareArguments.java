package com.liyc.mqs.common;

import com.liyc.mqs.mqserver.core.ExchangeType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 针对每个 VirtualHost 提供的⽅法, 都需要有⼀个类表⽰对应的参数
 *
 * @author Liyc
 * @date 2024/12/25 17:59
 **/

public class ExchangeDeclareArguments extends BasicArguments implements Serializable {

    private String exchangeName;

    private ExchangeType type;

    private boolean durable;

    private boolean autoDelete;

    private HashMap<String, Object> arguments;

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public ExchangeType getType() {
        return type;
    }

    public void setType(ExchangeType type) {
        this.type = type;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    public HashMap<String, Object> getArguments() {
        return arguments;
    }

    public void setArguments(HashMap<String, Object> arguments) {
        this.arguments = arguments;
    }
}

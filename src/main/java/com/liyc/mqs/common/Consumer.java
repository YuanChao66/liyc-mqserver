package com.liyc.mqs.common;

import com.liyc.mqs.mqserver.core.BasicProperties;

import java.io.IOException;

/**
 * 这是一个单纯的函数式接口(回调函数). 收到消息之后要处理消息时调用的方法.
 */
@FunctionalInterface
public interface Consumer {
    // 这里的方法名和参数, 也都是参考 RabbitMQ 展开的
    void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] bytes) throws IOException;
}

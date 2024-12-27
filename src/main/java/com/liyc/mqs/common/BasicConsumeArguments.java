package com.liyc.mqs.common;

import java.io.Serializable;

/**
 * 针对每个 VirtualHost 提供的⽅法, 都需要有⼀个类表⽰对应的参数
 *
 * @author Liyc
 * @date 2024/12/25 18:00
 **/

public class BasicConsumeArguments extends BasicArguments implements Serializable {
    private String consumerTag;
    private String queueName;
    private boolean autoAck;
    // 这个类对应的 basicConsume 方法中, 还有一个参数, 是回调函数. (如何来处理消息)
    // 这个回调函数, 是不能通过网络传输的.
    // 站在 broker server 这边, 针对消息的处理回调, 其实是统一的. (把消息返回给客户端)
    // 客户端这边收到消息之后, 再在客户端自己这边执行一个用户自定义的回调就行了.
    // 此时, 客户端也就不需要把自身的回调告诉给服务器了.
    // 这个类就不需要 consumer 成员了.

    public String getConsumerTag() {
        return consumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }
}

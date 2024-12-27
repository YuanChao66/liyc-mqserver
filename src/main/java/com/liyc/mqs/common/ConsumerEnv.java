package com.liyc.mqs.common;

/**
 * 消费者基础信息
 *
 * @author Liyc
 * @date 2024/12/24 17:07
 **/

public class ConsumerEnv {
    //1.消费者标识
    private String consumerTag;
    //2.关联队列名称
    private String queueName;
    //3.是否自动确认消息
    private boolean autoAck;
    //4.消费者对象
    private Consumer consumer;

    public ConsumerEnv(String consumerTag, String queueName, boolean autoAck, Consumer consumer) {
        this.consumerTag = consumerTag;
        this.queueName = queueName;
        this.autoAck = autoAck;
        this.consumer = consumer;
    }

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

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }
}

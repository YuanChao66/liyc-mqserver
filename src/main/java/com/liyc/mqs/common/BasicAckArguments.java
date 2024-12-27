package com.liyc.mqs.common;

import java.io.Serializable;

/**
 * 返回类父类
 * 属性：1.队列名称 2.消息id
 *
 * @author Liyc
 * @date 2024/12/25 17:57
 **/

public class BasicAckArguments extends BasicArguments implements Serializable {
    private String queueName;

    private String messageId;

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
}

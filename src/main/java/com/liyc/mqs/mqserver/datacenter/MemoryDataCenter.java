package com.liyc.mqs.mqserver.datacenter;

import com.liyc.mqs.mqserver.core.Binding;
import com.liyc.mqs.mqserver.core.Exchange;
import com.liyc.mqs.mqserver.core.Message;
import com.liyc.mqs.mqserver.core.Queue;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存消息数据处理类
 *
 * @author Liyc
 * @date 2024/12/20 15:56
 **/

public class MemoryDataCenter {
    // key 是 exchangeName, value 是 Exchange 对象
    private ConcurrentHashMap<String, Exchange> exchangeMap = new ConcurrentHashMap<>();
    // key 是 queueName, value 是 Queue 对象
    private ConcurrentHashMap<String, Queue> queueMap = new ConcurrentHashMap<>();
    //binging绑定，第一个key是ExchangeName交换机，第二个key是queueName队列
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Binding>> bindingMap = new ConcurrentHashMap<>();
    // key 是 messageId, value 是 Message 对象
    private ConcurrentHashMap<String, Message> messageMap = new ConcurrentHashMap<>();
    //第一个key是queueName队列，value是消息链表(一个哈希表+一个链表)
    private ConcurrentHashMap<String, LinkedList<Message>> queueMsg = new ConcurrentHashMap<>();
    //第一个key是queueName队列，第二个key是messageId待确认消息ID，value是待确认消息（一个哈希表+一个哈希表）
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> queueWaitMsg = new ConcurrentHashMap<>();

    /**
     * Exchange处理方法
     * 1.新增
     * 2.查询
     * 3.删除
     */
    public void insertExchange(Exchange exchange) {
        exchangeMap.put(exchange.getName(), exchange);
    }
    public Exchange selectExchange(String exchangeName) {
        return exchangeMap.get(exchangeName);
    }
    public void deleteExchange(String exchangeName) {
        exchangeMap.remove(exchangeName);
    }

    /**
     * Queue处理方法
     * 1.新增
     * 2.查询
     * 3.删除
     */
    public void insertQueue(Queue queue) {
        queueMap.put(queue.getName(), queue);
    }
    public Queue selectQueue(String queueName) {
        return queueMap.get(queueName);
    }
    public void deleteQueue(String queueName) {
        queueMap.remove(queueName);
    }

    /**
     * Binging处理方法
     * 1.新增
     * 2.查询-两个方法，exchanegName和queueName来查询唯一；exchangeName查询
     * 3.删除
     */
    public void insertBinding(Binding binding) {
        //先查看这个exchange存在不，不存在就新建
        ConcurrentHashMap<String, Binding> bing = bindingMap.computeIfAbsent(binding.getExchangeName(), k -> new ConcurrentHashMap<>());
        synchronized (bing) {
            if (bing.get(binding.getQueueName()) != null) {
                //exchange是否存在已经对应的这个queueName，如果存在就报错提示
            }

            bing.put(binding.getQueueName(), binding);
        }

    }
    public Binding selectBindingExQ(String exchangeName, String queueName) {
        ConcurrentHashMap<String, Binding> bing = bindingMap.get(exchangeName);
        if (bing == null) {
            //提示没有此binding
        }
        return bing.get(queueName);
    }
    public ConcurrentHashMap<String, Binding> selectBindingEx(String exchangeName) {
        return bindingMap.get(exchangeName);
    }
    public void deleteBinding(Binding binding) {
        ConcurrentHashMap<String, Binding> bing = bindingMap.get(binding.getExchangeName());
        if (bing == null) {
            //提示没有此bing
        }
        bing.remove(binding.getQueueName());
    }

    /**
     * Message处理方法
     * 1.新增：
     * 2.查询
     * 3.删除
     */
    public void insertMessage(Message message) {
        messageMap.put(message.getMessageId(), message);
    }
    public Message selectMessage(String messageID) {
        return messageMap.get(messageID);
    }
    public void deleteMessage(String messageID) {
        messageMap.remove(messageID);
    }

    /**
     * queueMsg处理方法
     * 1.新增：
     * 2.查询
     * 3.获取队列的消息个数
     */
    public void sendQueueMsg(Queue queue, Message message) {
        LinkedList<Message> messages = queueMsg.computeIfAbsent(queue.getName(), k -> new LinkedList<>());
        synchronized (messages) {
            messages.add(message);
        }
        // 在这里把该消息也往消息中心中插入一下. 假设如果 message 已经在消息中心存在, 重复插入也没关系.
        // 主要就是相同 messageId, 对应的 message 的内容一定是一样的. (服务器代码不会对 Message 内容做修改 basicProperties 和 body)
        insertMessage(message);
    }
    public Message pollMessage(String queueName) {
        LinkedList<Message> messages = queueMsg.get(queueName);
        if (messages == null) {
            return null;
        }
        synchronized (messages) {
            // 如果没找到, 说明队列中没有任何消息.
            if (messages.size() == 0) {
                return null;
            }
            // 链表中有元素, 就进行头删.
            Message message = messages.remove(0);
            return message;
        }
    }
    public int getMessageCount(String queueName) {
        LinkedList<Message> messages = queueMsg.get(queueName);
        if (messages == null) {
            return 0;
        }
        synchronized (messages) {
            return messages.size();
        }
    }

    /**
     * queueWaitMsg处理方法
     * 1.新增
     * 2.查询
     * 3.删除
     */
    public void insertMessageAck(String queueName, Message message) {
        ConcurrentHashMap<String, Message> messagesAck = queueWaitMsg.computeIfAbsent(queueName, k -> new ConcurrentHashMap<>());
        synchronized (messagesAck) {
            if (messagesAck.get(message.getMessageId()) != null) {
                //提示消息已存在
            }
            messagesAck.put(message.getMessageId(), message);
        }
    }
    public Message selectMessageAck(String queueName, String messageID) {
        ConcurrentHashMap<String, Message> messagesAck = queueWaitMsg.get(queueName);
        if (messagesAck == null) {
            return null;
        }
        return messagesAck.get(messageID);
    }
    public void deleteMeeageAck(String queueName, String messageID) {
        ConcurrentHashMap<String, Message> messageAck = queueWaitMsg.get(queueName);
        if (messageAck == null) {
            //提示无此消息
        }
        messageAck.remove(messageID);
    }

    // 这个方法就是从硬盘上读取数据, 把硬盘中之前持久化存储的各个维度的数据都恢复到内存中.
    public void recovery(DiskDataCenter diskDataCenter) throws IOException, ClassNotFoundException {
        // 0. 清空之前的所有数据
        exchangeMap.clear();
        queueMap.clear();
        bindingMap.clear();
        messageMap.clear();
        queueMsg.clear();

        // 1. 恢复所有的交换机数据
        List<Exchange> exchanges = diskDataCenter.selectAllExchanges();
        for (Exchange exchange : exchanges) {
            exchangeMap.put(exchange.getName(), exchange);
        }
        // 2. 恢复所有的队列数据
        List<Queue> queues = diskDataCenter.selectAllQueues();
        for (Queue queue : queues) {
            queueMap.put(queue.getName(), queue);
        }
        // 3. 恢复所有的绑定数据
        List<Binding> bindings = diskDataCenter.selectAllBindings();
        for (Binding binding : bindings) {
            ConcurrentHashMap<String, Binding> bindingTmp = bindingMap.computeIfAbsent(binding.getExchangeName(),
                k -> new ConcurrentHashMap<>());
            bindingTmp.put(binding.getQueueName(), binding);
        }
        // 4. 恢复所有的消息数据
        //    遍历所有的队列, 根据每个队列的名字, 获取到所有的消息.
        for (Queue queue : queues) {
            LinkedList<Message> messages = diskDataCenter.loadAllMessageFromQueue(queue.getName());
            queueMsg.put(queue.getName(), messages);
            for (Message message : messages) {
                    messageMap.put(message.getMessageId(), message);
            }
        }
        // 注意!! 针对 "未确认的消息" 这部分内存中的数据, 不需要从硬盘恢复. 之前考虑硬盘存储的时候, 也没设定这一块.
        // 一旦在等待 ack 的过程中, 服务器重启了, 此时这些 "未被确认的消息", 就恢复成 "未被取走的消息" .
        // 这个消息在硬盘上存储的时候, 就是当做 "未被取走"

    }
}

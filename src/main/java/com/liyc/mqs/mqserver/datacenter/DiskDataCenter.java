package com.liyc.mqs.mqserver.datacenter;

import com.liyc.mqs.mqserver.core.Binding;
import com.liyc.mqs.mqserver.core.Exchange;
import com.liyc.mqs.mqserver.core.Message;
import com.liyc.mqs.mqserver.core.Queue;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 数据库和文件整合类
 *
 * @author Liyc
 * @date 2024/12/20 15:25
 **/

public class DiskDataCenter {
    // 这个实例用来管理数据库中的数据
    private DataManager dataManager = new DataManager();
    // 这个实例用来管理数据文件中的数据
    private MessageFileManager messageFileManager = new MessageFileManager();

    public void init() {
        dataManager.init();

        messageFileManager.init();
    }

    // 封装交换机操作
    public void insertExchange(Exchange exchange) {
        dataManager.insertExchange(exchange);
    }

    public void deleteExchange(String exchangeName) {
        dataManager.deleteExchange(exchangeName);
    }

    public List<Exchange> selectAllExchanges() {
        return dataManager.selectAllExchanges();
    }

    // 封装队列操作
    public void insertQueue(Queue queue) throws IOException {
        dataManager.insertQueue(queue);
        // 创建队列的同时, 不仅仅是把队列对象写到数据库中, 还需要创建出对应的目录和文件
        messageFileManager.initMsg(queue.getName());
    }

    public void deleteQueue(String queueName) throws IOException {
        dataManager.deleteQueue(queueName);
        // 删除队列的同时, 不仅仅是把队列从数据库中删除, 还需要删除对应的目录和文件
        messageFileManager.deleteFile(queueName);
    }

    public List<Queue> selectAllQueues() {
        return dataManager.selectAllQueues();
    }

    // 封装绑定操作
    public void insertBinding(Binding binding) {
        dataManager.insertBinding(binding);
    }

    public void deleteBinding(Binding binding) {
        dataManager.deleteBinding(binding);
    }

    public List<Binding> selectAllBindings() {
        return dataManager.selectAllBindings();
    }

    // 封装消息操作
    public void sendMessage(Queue queue, Message message) throws IOException {
        messageFileManager.saveMsgFile(queue, message);
    }

    public void deleteMessage(Queue queue, Message message) throws IOException, ClassNotFoundException {
        messageFileManager.deleteMsgData(queue, message);
        if (messageFileManager.isFlagGC(queue.getName())) {
                messageFileManager.gcMsgData(queue);
        }
    }

    public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, ClassNotFoundException {
        return messageFileManager.initAllMsg(queueName);
    }
}

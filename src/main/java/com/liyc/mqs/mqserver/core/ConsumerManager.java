package com.liyc.mqs.mqserver.core;

import com.liyc.mqs.common.Consumer;
import com.liyc.mqs.common.ConsumerEnv;
import com.liyc.mqs.mqserver.VirtualHost;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 消息消费处理类
 * 1.初始轮询查询消息
 * 2.消费消息
 * 3.添加订阅者
 * 4.获取令牌
 *
 * @author Liyc
 * @date 2024/12/24 17:18
 **/

public class ConsumerManager {
    //持有上层的 VirtualHost 对象的引用. 用来操作数据.
    private VirtualHost prent;
    //线程池，批量消费消息
    private ExecutorService workPool = Executors.newFixedThreadPool(4);;
    //阻塞队列，线程池获取令牌，有令牌就通过轮询线程去查询此队列
    private BlockingQueue<String> tokenQueue = new LinkedBlockingQueue<>();
    //轮询线程，轮询读取消息
    private Thread thread = null;

    //初始化就启动轮询查询线程
    public ConsumerManager(VirtualHost v) {
        prent = v;
        thread = new Thread(() -> {
            while (true) {
                try {
                    String queueName = tokenQueue.take();
                    Queue queue = prent.getMemoryDataCenter().selectQueue(queueName);
                    if (Objects.isNull(queue)) {
                        //无此队列
                    }
                    //消费消息
                    sendConsumer(queue);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 获取令牌
     * @param queueName
     * @throws InterruptedException
     */
    public void notifyConsume(String queueName) throws InterruptedException {
        tokenQueue.put(queueName);
    }

    /**
     * 添加订阅者
     * 1.订阅消费者到队列
     * 2.消费当前队列的消息
     * @param consumerTag
     * @param queueName
     * @param autoAck
     * @param consumer
     */
    public void addConsumer(String consumerTag, String queueName, boolean autoAck, Consumer consumer) {
        //1.订阅消费者到队列
        ConsumerEnv consumerEnv = new ConsumerEnv(consumerTag, queueName, autoAck, consumer);
        Queue queue = prent.getMemoryDataCenter().selectQueue(queueName);
        if (Objects.isNull(queue)) {
            //无此队列
        }
        synchronized (queue) {
            queue.setConsumers(consumerEnv);
            //查询当前队列有多少个消息
            int messageCount = prent.getMemoryDataCenter().getMessageCount(queueName);
            for (int i=0; i<=messageCount; i++) {
                //初始消费当列存在的消息，一个消息消费一次
                sendConsumer(queue);
            }
        }

    }

    /**
     * 消费消息
     * 1.找到消费者
     * 2.获取消息
     * 3.线程池回调消费者消费方法
     * 3.1消息放入待确认
     * 3.2回调消费方法
     * 3.3.1自动确认：删除待确认消息，删除硬盘消息，删除内存消息
     * 3.3.2手动确认：等待手动调用basicAck方法获取结果
     * @param queue
     */
    public void sendConsumer(Queue queue) {
        //1.找到消费者
        ConsumerEnv consumerEnv = queue.chooseConsumers();
        if (Objects.isNull(consumerEnv)) {
            //无订阅消费者
        }
        //2.获取消息
        Message message = prent.getMemoryDataCenter().pollMessage(queue.getName());
        //3.线程池回调消费者消费方法
        workPool.submit(() -> {
            try {
                //3.1消息放入得确认
                prent.getMemoryDataCenter().insertMessageAck(queue.getName(), message);
                //3.2回调消费方法
                consumerEnv.getConsumer().handleDelivery(consumerEnv.getConsumerTag(), message.getBasicProperties(), message.getBody());
                //3.3消息确认
                if (consumerEnv.isAutoAck()) {
                    //3.3.1自动确认：删除待确认消息，删除硬盘消息，删除内存消息
                    prent.getMemoryDataCenter().deleteMeeageAck(queue.getName(), message.getMessageId());
                    // Durable 为 1 , 不持久化. Durable 为 2 表示持久化.
                    if (message.getDurable() == 2) {
                        prent.getDiskDataCenter().deleteMessage(queue, message);
                    }
                    prent.getMemoryDataCenter().deleteMeeageAck(queue.getName(), message.getMessageId());
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        });
    }
}

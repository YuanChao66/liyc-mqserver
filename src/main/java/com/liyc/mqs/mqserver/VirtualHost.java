package com.liyc.mqs.mqserver;

import com.liyc.mqs.common.Consumer;
import com.liyc.mqs.common.MqException;
import com.liyc.mqs.mqserver.core.*;
import com.liyc.mqs.mqserver.datacenter.DiskDataCenter;
import com.liyc.mqs.mqserver.datacenter.MemoryDataCenter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * VirtualHost空间处理类-核心API
 * 创建交换机-exchangeDeclare
 * 删除交换机-exchangeDelete
 * 创建队列-queueDeclare
 * 删除队列-queueDelete
 * 创建绑定-bindingDeclare
 * 删除绑定-bindingDelete
 *
 * 发布消息-basicPublic
 * 订阅消息-basicCosume
 * 确认消息-basicAck
 *
 * @author Liyc
 * @date 2024/12/23 14:53
 **/

public class VirtualHost {
    //虚拟机名称
    private String virtualHostName;
    //数据库消息处理类
    private DiskDataCenter diskDataCenter = new DiskDataCenter();
    //内存消息处理类
    private MemoryDataCenter memoryDataCenter = new MemoryDataCenter();
    //路由转发规则类
    private Router router = new Router();
    private Object exchangeLock = new Object();
    private Object queueLock = new Object();
    //消费者处理类
    private ConsumerManager consumerManager = new ConsumerManager(this);

    public String getVirtualHostName() {
        return virtualHostName;
    }
    public DiskDataCenter getDiskDataCenter() {
        return diskDataCenter;
    }
    public MemoryDataCenter getMemoryDataCenter() {
        return memoryDataCenter;
    }
    public VirtualHost(String virtualHostName) {
        this.virtualHostName = virtualHostName;

        //1.初始化数据库，需要创建文件和数据库表，不用初始化内存消息
        diskDataCenter.init();

        //2.需要把本地消息恢复到内存中
        try {
            memoryDataCenter.recovery(diskDataCenter);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    //创建交换机-exchangeDeclare
    public boolean exchangeDeclare(String exchangeName, ExchangeType exchangeType, boolean durable, boolean autoDelete, HashMap<String, Object> arguments) {
        synchronized (exchangeLock) {
            exchangeName = virtualHostName + exchangeName;

            Exchange existExchange = memoryDataCenter.selectExchange(exchangeName);
            if (Objects.nonNull(existExchange)) {
                System.out.println("[VirtualHost] 交换机已经存在! exchangeName=" + exchangeName);
                return true;
            }
            Exchange exchange = new Exchange();
            exchange.setName(exchangeName);
            exchange.setType(exchangeType);
            exchange.setAutoDelete(autoDelete);
            exchange.setDurable(durable);
            exchange.setArguments(arguments);

            //先插入本地，再写入内存
            if (durable) {
                diskDataCenter.insertExchange(exchange);
            }

            memoryDataCenter.insertExchange(exchange);
            return true;
        }


    }
    //删除交换机-exchangeDelete
    public boolean exchangeDelete(String exchangeName) {
        synchronized (exchangeLock) {
            exchangeName = virtualHostName + exchangeName;
            Exchange existExchange = memoryDataCenter.selectExchange(exchangeName);
            if (Objects.isNull(existExchange)) {
                System.out.println("[VirtualHost] 交换机不存在! exchangeName=" + exchangeName);
                return true;
            }
            if (existExchange.isDurable()) {
                diskDataCenter.deleteExchange(exchangeName);
            }

            memoryDataCenter.deleteExchange(exchangeName);
            return true;
        }
    }
    //创建队列-queueDeclare
    public boolean queueDeclare(String queueName, boolean exclusive, boolean durable, boolean autoDelete, HashMap<String, Object> arguments) {
        try {
            synchronized (queueLock) {
                queueName = virtualHostName + queueName;
                Queue existQueue = memoryDataCenter.selectQueue(queueName);
                if (Objects.nonNull(existQueue)) {
                    System.out.println("[VirtualHost] 队列已经存在! queueName=" + queueName);
                    return true;
                }
                Queue queue = new Queue();
                queue.setName(queueName);
                queue.setExclusive(exclusive);
                queue.setDurable(durable);
                queue.setAutoDelete(autoDelete);
                queue.setArguments(arguments);
                if (durable) {
                    diskDataCenter.insertQueue(queue);
                }

                memoryDataCenter.insertQueue(queue);
                return true;
            }
        } catch (IOException e) {
            return false;
        }
    }
    //删除队列-queueDelete
    public boolean queueDelete(String queueName) {
        try {
            synchronized (queueLock) {
                queueName = virtualHostName + queueName;
                Queue existQueue = memoryDataCenter.selectQueue(queueName);
                if (Objects.isNull(existQueue)) {
                    System.out.println("[VirtualHost] 队列不存在! queueName=" + queueName);
                    return true;
                }
                if (existQueue.isDurable()) {
                    diskDataCenter.deleteQueue(queueName);
                }
                memoryDataCenter.deleteQueue(queueName);
                return true;
            }
        } catch (IOException e) {
            return false;
        }


    }
    //创建绑定-bindingDeclare
    public boolean bindingDeclare(String exchangeName, String queueName, String bindingKey) {
        synchronized (exchangeLock) {
            synchronized (queueLock) {
                exchangeName = virtualHostName + exchangeName;
                queueName = virtualHostName + queueName;
                //1.判断绑定是否存在
                Binding existBinding = memoryDataCenter.selectBindingExQ(exchangeName, queueName);
                if (Objects.nonNull(existBinding)) {
                    System.out.println("[VirtualHost] 绑定已经存在! exchangeName=" + exchangeName + ",queueName=" + queueName);
                    return true;
                }
                //2.判断bindingKey规则是否正确
                if (!router.checkBindingKey(bindingKey)) {
                    return false;
                }

                //3.判断交换机是否存在
                Exchange exchange = memoryDataCenter.selectExchange(exchangeName);
                if (Objects.isNull(exchange)) {
                    System.out.println("[VirtualHost] 交换机不存在! exchangeName=" + exchangeName);
                    return false;
                }
                //4.判断队列是否存在
                Queue queue = memoryDataCenter.selectQueue(queueName);
                if (Objects.isNull(queue)) {
                    System.out.println("[VirtualHost] 队列不存在! queueName=" + queueName);
                    return false;
                }

                //5.写入硬盘
                Binding binding = new Binding();
                binding.setExchangeName(exchangeName);
                binding.setQueueName(queueName);
                binding.setBindingKey(bindingKey);
                if (exchange.isDurable() && queue.isDurable()) {
                    diskDataCenter.insertBinding(binding);
                }

                //6.写入内存
                memoryDataCenter.insertBinding(binding);
                return true;
            }
        }
    }
    //删除绑定-bindingDelete
    public boolean bindingDelete(String exchangeName, String queueName) {
        synchronized (exchangeLock) {
            synchronized (queueLock) {
                exchangeName = virtualHostName + exchangeName;
                queueName = virtualHostName + queueName;
                //1.判断绑定是否存在
                Binding existBinding = memoryDataCenter.selectBindingExQ(exchangeName, queueName);
                if (Objects.isNull(existBinding)) {
                    System.out.println("[VirtualHost] 绑定不存在! exchangeName=" + exchangeName + ",queueName=" + queueName);
                    return true;
                }
                //2.删除硬盘，无论绑定是否持久化了, 都尝试从硬盘删一下. 就算不存在, 这个删除也无副作用.
                diskDataCenter.deleteBinding(existBinding);
                //3.删除内存
                memoryDataCenter.deleteBinding(existBinding);
                return true;
            }
        }
    }
    /**发布消息-basicPublic
     * 1、校验routingKey的规则
     * 2、建立消息对象
     * 3、根据ExhcangeType来走不同的转发规则
     * 4、发送消息
     */
    public boolean basicPublic(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] bytes) {
        try {
            exchangeName = virtualHostName + exchangeName;
            //1.判断交换机是否存在
            Exchange exchange = memoryDataCenter.selectExchange(exchangeName);
            if (Objects.isNull(exchange)) {
                System.out.println("[VirtualHost] 交换机不存在! exchangeName=" + exchangeName);
                return false;
            }
            //2.校验routingKey的规则
            if (!router.checkRoutingKey(routingKey)) {
                return false;
            }
            //3.创建消息对象
            Message message = Message.createMessageWithID(routingKey, basicProperties, bytes);
            //4.根据ExhcangeType来走不同的转发规则
            if (ExchangeType.DIRECT.equals(exchange.getType())) {
                String queuqName = virtualHostName + routingKey;
                Queue queue = memoryDataCenter.selectQueue(queuqName);
                if (queue == null) {
                    //没有匹配的队列
                    return false;
                }
                //5.发送消息
                sendMessage(queue, message);
            } else {
                ConcurrentHashMap<String, Binding> bindingHashMap = memoryDataCenter.selectBindingEx(exchangeName);
                if (bindingHashMap == null) {
                    return false;
                }
                for (Map.Entry<String, Binding> bindingMap : bindingHashMap.entrySet()) {
                    Binding binding = bindingMap.getValue();
                    boolean route = router.route(exchange.getType(), binding, message);
                    if (!route) {
                        //没有匹配的队列
                        return false;
                    } else {
                        //5.发送消息
                        Queue queue = memoryDataCenter.selectQueue(bindingMap.getKey());
                        if (queue == null) {
                            //没有匹配的队列
                            return false;
                        }
                        sendMessage(queue,message);
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }
    //订阅消息-basicCosume
    //确认消息-basicAck

    //发送消息
    public void sendMessage(Queue queue, Message message) throws IOException, InterruptedException {
        // 1.如果需要持久化，就先持久化
        // Durable 为 1 , 不持久化. Durable 为 2 表示持久化.
        if (message.getDurable() == 2) {
            diskDataCenter.sendMessage(queue, message);
        }
        //2.存入内存
        memoryDataCenter.sendQueueMsg(queue,message);
        //3.通知消费者消费消息
        consumerManager.notifyConsume(queue.getName());
    }

    // 订阅消息.
    // 添加一个队列的订阅者, 当队列收到消息之后, 就要把消息推送给对应的订阅者.
    // consumerTag: 消费者的身份标识
    // autoAck: 消息被消费完成后, 应答的方式. 为 true 自动应答. 为 false 手动应答.
    // consumer: 是一个回调函数. 此处类型设定成函数式接口. 这样后续调用 basicConsume 并且传实参的时候, 就可以写作 lambda 样子了.
    public boolean basicConsume(String consumerTag, String queueName, boolean autoAck, Consumer consumer) {
        // 构造一个 ConsumerEnv 对象, 把这个对应的队列找到, 再把这个 Consumer 对象添加到该队列中.
        queueName = virtualHostName + queueName;
        try {
            consumerManager.addConsumer(consumerTag, queueName, autoAck, consumer);
            System.out.println("[VirtualHost] basicConsume 成功! queueName=" + queueName);
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] basicConsume 失败! queueName=" + queueName);
            e.printStackTrace();
            return false;
        }
    }

    //确认消息方法
    public boolean basicAck(String queueName, String messageId) {
        queueName = virtualHostName + queueName;
        try {
            // 1. 获取到消息和队列
            Message message = memoryDataCenter.selectMessage(messageId);
            if (message == null) {
                throw new MqException("[VirtualHost] 要确认的消息不存在! messageId=" + messageId);
            }
            Queue queue = memoryDataCenter.selectQueue(queueName);
            if (queue == null) {
                throw new MqException("[VirtualHost] 要确认的队列不存在! queueName=" + queueName);
            }
            // 2. 删除硬盘上的数据
            if (message.getDurable() == 2) {
                diskDataCenter.deleteMessage(queue, message);
            }
            // 3. 删除消息中心中的数据
            memoryDataCenter.deleteMessage(messageId);
            // 4. 删除待确认的集合中的数据
            memoryDataCenter.deleteMeeageAck(queueName, messageId);
            System.out.println("[VirtualHost] basicAck 成功! 消息被成功确认! queueName=" + queueName
                    + ", messageId=" + messageId);
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] basicAck 失败! 消息确认失败! queueName=" + queueName
                        + ", messageId=" + messageId);
            e.printStackTrace();
            return false;
        }
    }
}

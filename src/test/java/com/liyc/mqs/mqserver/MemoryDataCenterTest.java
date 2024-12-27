package com.liyc.mqs.mqserver;

import com.liyc.mqs.MqsApplication;
import com.liyc.mqs.mqserver.core.*;
import com.liyc.mqs.mqserver.datacenter.DiskDataCenter;
import com.liyc.mqs.mqserver.datacenter.MemoryDataCenter;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存数据测试类
 *
 * @author Liyc
 * @date 2024/12/23 11:14
 **/

@SpringBootTest
public class MemoryDataCenterTest {
    private MemoryDataCenter memoryDataCenter = null;

    @BeforeEach
    public void setUp() {
        memoryDataCenter = new MemoryDataCenter();
    }

    @AfterEach
    public void tearDown() {
        memoryDataCenter = null;
    }

    // 创建一个测试交换机
    private Exchange createTestExchange(String exchangeName) {
        Exchange exchange = new Exchange();
        exchange.setName(exchangeName);
        exchange.setType(ExchangeType.DIRECT);
        exchange.setAutoDelete(false);
        exchange.setDurable(true);
        return exchange;
    }

    // 创建一个测试队列
    private Queue createTestQueue(String queueName) {
        Queue queue = new Queue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setExclusive(false);
        queue.setAutoDelete(false);
        return queue;
    }

    // 针对交换机进行测试
    @Test
    public void testExchange() {
        // 1. 先构造一个交换机并插入.
        Exchange expectedExchange = createTestExchange("testExchange");
        memoryDataCenter.insertExchange(expectedExchange);
        // 2. 查询出这个交换机, 比较结果是否一致. 此处直接比较这俩引用指向同一个对象.
        Exchange actualExchange = memoryDataCenter.selectExchange("testExchange");
        Assertions.assertEquals(expectedExchange, actualExchange);
        // 3. 删除这个交换机
        memoryDataCenter.deleteExchange("testExchange");
        // 4. 再查一次, 看是否就查不到了
        actualExchange = memoryDataCenter.selectExchange("testExchange");
        Assertions.assertNull(actualExchange);
    }

    // 针对队列进行测试
    @Test
    public void testQueue() {
        // 1. 构造一个队列, 并插入
        Queue expectedQueue = createTestQueue("testQueue");
        memoryDataCenter.insertQueue(expectedQueue);
        // 2. 查询这个队列, 并比较
        Queue actualQueue = memoryDataCenter.selectQueue("testQueue");
        Assertions.assertEquals(expectedQueue, actualQueue);
        // 3. 删除这个队列
        memoryDataCenter.deleteQueue("testQueue");
        // 4. 再次查询队列, 看是否能查到
        actualQueue = memoryDataCenter.selectQueue("testQueue");
        Assertions.assertNull(actualQueue);
    }

    // 针对绑定进行测试
    @Test
    public void testBinding() {
        Binding expectedBinding = new Binding();
        expectedBinding.setExchangeName("testExchange");
        expectedBinding.setQueueName("testQueue");
        expectedBinding.setBindingKey("testBindingKey");
        memoryDataCenter.insertBinding(expectedBinding);

        Binding actualBinding = memoryDataCenter.selectBindingExQ("testExchange", "testQueue");
        Assertions.assertEquals(expectedBinding, actualBinding);

        ConcurrentHashMap<String, Binding> bindingMap = memoryDataCenter.selectBindingEx("testExchange");
        Assertions.assertEquals(1, bindingMap.size());
        Assertions.assertEquals(expectedBinding, bindingMap.get("testQueue"));

        memoryDataCenter.deleteBinding(expectedBinding);

        actualBinding = memoryDataCenter.selectBindingExQ("testExchange", "testQueue");
        Assertions.assertNull(actualBinding);
    }

    private Message createTestMessage(String content) {
        Message message = Message.createMessageWithID("testRoutingKey", null, content.getBytes());
        return message;
    }

    @Test
    public void testMessage() {
        Message expectedMessage = createTestMessage("testMessage");
        memoryDataCenter.insertMessage(expectedMessage);

        Message actualMessage = memoryDataCenter.selectMessage(expectedMessage.getMessageId());
        Assertions.assertEquals(expectedMessage, actualMessage);

        memoryDataCenter.deleteMessage(expectedMessage.getMessageId());

        actualMessage = memoryDataCenter.selectMessage(expectedMessage.getMessageId());
        Assertions.assertNull(actualMessage);
    }

    @Test
    public void testSendMessage() {
        // 1. 创建一个队列, 创建 10 条消息, 把这些消息都插入队列中.
        Queue queue = createTestQueue("testQueue");
        List<Message> expectedMessages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = createTestMessage("testMessage" + i);
            memoryDataCenter.sendQueueMsg(queue, message);
            expectedMessages.add(message);
        }

        // 2. 从队列中取出这些消息.
        List<Message> actualMessages = new ArrayList<>();
        while (true) {
            Message message = memoryDataCenter.pollMessage("testQueue");
            if (message == null) {
                break;
            }
            actualMessages.add(message);
        }

        // 3. 比较取出的消息和之前的消息是否一致.
        Assertions.assertEquals(expectedMessages.size(), actualMessages.size());
        for (int i = 0; i < expectedMessages.size(); i++) {
            Assertions.assertEquals(expectedMessages.get(i), actualMessages.get(i));
        }
    }

    @Test
    public void testMessageWaitAck() {
        Message expectedMessage = createTestMessage("expectedMessage");
        memoryDataCenter.insertMessageAck("testQueue", expectedMessage);

        Message actualMessage = memoryDataCenter.selectMessageAck("testQueue", expectedMessage.getMessageId());
        Assertions.assertEquals(expectedMessage, actualMessage);

        memoryDataCenter.deleteMeeageAck("testQueue", expectedMessage.getMessageId());
        actualMessage = memoryDataCenter.selectMessageAck("testQueue", expectedMessage.getMessageId());
        Assertions.assertNull(actualMessage);
    }

    @Test
    public void testRecovery() throws IOException, ClassNotFoundException {
        // 由于后续需要进行数据库操作, 依赖 MyBatis. 就需要先启动 SpringApplication, 这样才能进行后续的数据库操作.
        MqsApplication.context = SpringApplication.run(MqsApplication.class);

        // 1. 在硬盘上构造好数据
        DiskDataCenter diskDataCenter = new DiskDataCenter();
        diskDataCenter.init();

        // 构造交换机
        Exchange expectedExchange = createTestExchange("testExchange");
        diskDataCenter.insertExchange(expectedExchange);

        // 构造队列
        Queue expectedQueue = createTestQueue("testQueue");
        diskDataCenter.insertQueue(expectedQueue);

        // 构造绑定
        Binding expectedBinding = new Binding();
        expectedBinding.setExchangeName("testExchange");
        expectedBinding.setQueueName("testQueue");
        expectedBinding.setBindingKey("testBindingKey");
        diskDataCenter.insertBinding(expectedBinding);

        // 构造消息
        Message expectedMessage = createTestMessage("testContent");
        diskDataCenter.sendMessage(expectedQueue, expectedMessage);

        // 2. 执行恢复操作
        memoryDataCenter.recovery(diskDataCenter);

        // 3. 对比结果
        Exchange actualExchange = memoryDataCenter.selectExchange("testExchange");
        Assertions.assertEquals(expectedExchange.getName(), actualExchange.getName());
        Assertions.assertEquals(expectedExchange.getType(), actualExchange.getType());
        Assertions.assertEquals(expectedExchange.isDurable(), actualExchange.isDurable());
        Assertions.assertEquals(expectedExchange.isAutoDelete(), actualExchange.isAutoDelete());

        Queue actualQueue = memoryDataCenter.selectQueue("testQueue");
        Assertions.assertEquals(expectedQueue.getName(), actualQueue.getName());
        Assertions.assertEquals(expectedQueue.isDurable(), actualQueue.isDurable());
        Assertions.assertEquals(expectedQueue.isAutoDelete(), actualQueue.isAutoDelete());
        Assertions.assertEquals(expectedQueue.isExclusive(), actualQueue.isExclusive());

        Binding actualBinding = memoryDataCenter.selectBindingExQ("testExchange", "testQueue");
        Assertions.assertEquals(expectedBinding.getExchangeName(), actualBinding.getExchangeName());
        Assertions.assertEquals(expectedBinding.getQueueName(), actualBinding.getQueueName());
        Assertions.assertEquals(expectedBinding.getBindingKey(), actualBinding.getBindingKey());

        Message actualMessage = memoryDataCenter.pollMessage("testQueue");
        Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
        Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
        Assertions.assertEquals(expectedMessage.getDurable(), actualMessage.getDurable());
        Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());

        // 4. 清理硬盘的数据, 把整个 data 目录里的内容都删掉(包含了 meta.db 和 队列的目录).
        MqsApplication.context.close();
        File dataDir = new File("./data");
        FileUtils.deleteDirectory(dataDir);
    }
}

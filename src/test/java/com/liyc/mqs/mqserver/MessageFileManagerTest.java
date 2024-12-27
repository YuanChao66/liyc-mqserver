package com.liyc.mqs.mqserver;

import com.liyc.mqs.MqsApplication;
import com.liyc.mqs.mqserver.core.Message;
import com.liyc.mqs.mqserver.core.Queue;
import com.liyc.mqs.mqserver.datacenter.MessageFileManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 文件操作测试类
 *
 * @author Liyc
 * @date 2024/12/20 10:10
 **/
@SpringBootTest
public class MessageFileManagerTest {
    private MessageFileManager messageFileManager = new MessageFileManager();
    private final String testQueueName1 = "testQueue1";
    private final String testQueueName2 = "testQueue2";

    //测试初始要把文件创建出来
    @BeforeEach
    private void createFile() {
        MqsApplication.context = SpringApplication.run(MqsApplication.class);
        try {
            messageFileManager.initMsg(testQueueName1);
            messageFileManager.initMsg(testQueueName2);
        } catch (IOException e) {

        }
    }
    //测试结束要删除文件
    @AfterEach
    private void deleteFile(){
        messageFileManager.deleteFile(testQueueName1);
        messageFileManager.deleteFile(testQueueName2);
        MqsApplication.context.close();
    }

    //测试创建文件
    @Test
    public void createQueueFile() {
        // 创建队列文件已经在上面 setUp 阶段执行过了. 此处主要是验证看看文件是否存在.
        File queueDataFile1 = new File("./data/" + testQueueName1 + "/queue_data.txt");
        Assertions.assertEquals(true, queueDataFile1.isFile());
        File queueStatFile1 = new File("./data/" + testQueueName1 + "/queue_stat.txt");
        Assertions.assertEquals(true, queueStatFile1.isFile());

        File queueDataFile2 = new File("./data/" + testQueueName2 + "/queue_data.txt");
        Assertions.assertEquals(true, queueDataFile2.isFile());
        File queueStatFile2 = new File("./data/" + testQueueName2 + "/queue_stat.txt");
        Assertions.assertEquals(true, queueStatFile2.isFile());
    }

    //测试stat读写
    @Test
    public void writereadFile() {
        MessageFileManager.Stat stat = new MessageFileManager.Stat();
        stat.sumMsg = 1000;
        stat.countMsg = 483;
        messageFileManager.writeMsgCount(testQueueName1, stat);

        MessageFileManager.Stat newStat = messageFileManager.readMsgCount(testQueueName1);
        Assertions.assertEquals(1000, newStat.sumMsg);
        Assertions.assertEquals(483, newStat.countMsg);
        System.out.println("测试 readStat 和 writeStat 完成!");
    }

    //创建队列
    public Queue createQueue(String queueName) {
        Queue queue = new Queue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setAutoDelete(true);
        queue.setExclusive(false);
        return queue;
    }

    //创建消息
    public Message createMessage(String content) {
        Message message = Message.createMessageWithID("testRoutingKey", null, content.getBytes());
        return message;
    }

    //测试写入消息
    @Test
    public void sendMessage() throws IOException, ClassNotFoundException {
        Queue queue = createQueue(testQueueName1);
        Message message = createMessage("testMessage");

        messageFileManager.saveMsgFile(queue, message);

        MessageFileManager.Stat stat = messageFileManager.readMsgCount(testQueueName1);
        Assertions.assertEquals(1, stat.countMsg);
        Assertions.assertEquals(1, stat.sumMsg);

        LinkedList<Message> messages = messageFileManager.initAllMsg(testQueueName1);
        Assertions.assertEquals(1, messages.size());
        Message messageOld = messages.get(0);
        Assertions.assertEquals(message.getMessageId(), messageOld.getMessageId());
        Assertions.assertEquals(message.getDurable(), messageOld.getDurable());
        Assertions.assertEquals(message.getRoutingKey(), messageOld.getRoutingKey());
        Assertions.assertArrayEquals(message.getBody(), messageOld.getBody());
    }

    //测试删除消息
    @Test
    public void testDeleteMessage() throws IOException, ClassNotFoundException {
        // 创建队列, 写入 10 个消息. 删除其中的几个消息. 再把所有消息读取出来, 判定是否符合预期.
        Queue queue = createQueue(testQueueName1);
        List<Message> expectedMessages = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
                Message message = createMessage("testMessage" + i);
            messageFileManager.saveMsgFile(queue, message);
            expectedMessages.add(message);
        }

        // 删除其中的三个消息
        messageFileManager.deleteMsgData(queue, expectedMessages.get(7));
        messageFileManager.deleteMsgData(queue, expectedMessages.get(8));
        messageFileManager.deleteMsgData(queue, expectedMessages.get(9));

        // 对比这里的内容是否正确.
        LinkedList<Message> actualMessages = messageFileManager.initAllMsg(testQueueName1);
        Assertions.assertEquals(7, actualMessages.size());
        for (int i = 0; i < actualMessages.size(); i++) {
            Message expectedMessage = expectedMessages.get(i);
            Message actualMessage = actualMessages.get(i);
            System.out.println("[" + i + "] actualMessage=" + actualMessage);

            Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDurable(), actualMessage.getDurable());
            Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());
            Assertions.assertEquals(0x1, actualMessage.getIsValid());
        }
    }

    //测试初始化加载内存
    @Test
    public void testLoadAllMessageFromQueue() throws IOException, ClassNotFoundException {
        // 往队列中插入 100 条消息, 然后验证看看这 100 条消息从文件中读取之后, 是否和最初是一致的.
        Queue queue = createQueue(testQueueName1);
        List<Message> expectedMessages = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            Message message = createMessage("testMessage" + i);
            messageFileManager.saveMsgFile(queue, message);
            expectedMessages.add(message);
        }

        // 读取所有消息
        LinkedList<Message> actualMessages = messageFileManager.initAllMsg(testQueueName1);
        Assertions.assertEquals(expectedMessages.size(), actualMessages.size());
        for (int i = 0; i < expectedMessages.size(); i++) {
            Message expectedMessage = expectedMessages.get(i);
            Message actualMessage = actualMessages.get(i);
            System.out.println("[" + i + "] actualMessage=" + actualMessage);

            Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDurable(), actualMessage.getDurable());
            Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());
            Assertions.assertEquals(0x1, actualMessage.getIsValid());
        }
    }

    //测试gc
    @Test
    public void testGC() throws IOException, ClassNotFoundException {
        // 先往队列中写 100 个消息. 获取到文件大小.
        // 再把 100 个消息中的一半, 都给删除掉(比如把下标为偶数的消息都删除)
        // 再手动调用 gc 方法, 检测得到的新的文件的大小是否比之前缩小了.
        Queue queue = createQueue(testQueueName1);
        List<Message> expectedMessages = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            Message message = createMessage("testMessage" + i);
            messageFileManager.saveMsgFile(queue, message);
            expectedMessages.add(message);
        }

        // 获取 gc 前的文件大小
        File beforeGCFile = new File("./data/" + testQueueName1 + "/queue_data.txt");
        long beforeGCLength = beforeGCFile.length();

        // 删除偶数下标的消息
        for (int i = 0; i < 100; i += 2) {
            messageFileManager.deleteMsgData(queue, expectedMessages.get(i));
        }

        // 手动调用 gc
        messageFileManager.gcMsgData(queue);

        // 重新读取文件, 验证新的文件的内容是不是和之前的内容匹配
        LinkedList<Message> actualMessages = messageFileManager.initAllMsg(testQueueName1);
        Assertions.assertEquals(50, actualMessages.size());
        for (int i = 0; i < actualMessages.size(); i++) {
            // 把之前消息偶数下标的删了, 剩下的就是奇数下标的元素了.
            // actual 中的 0 对应 expected 的 1
            // actual 中的 1 对应 expected 的 3
            // actual 中的 2 对应 expected 的 5
            // actual 中的 i 对应 expected 的 2 * i + 1
            Message expectedMessage = expectedMessages.get(2 * i + 1);
            Message actualMessage = actualMessages.get(i);

            Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDurable(), actualMessage.getDurable());
            Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());
            Assertions.assertEquals(0x1, actualMessage.getIsValid());
        }
        // 获取新的文件的大小
        File afterGCFile = new File("./data/" + testQueueName1 + "/queue_data.txt");
        long afterGCLength = afterGCFile.length();
        System.out.println("before: " + beforeGCLength);
        System.out.println("after: " + afterGCLength);
        Assertions.assertTrue(beforeGCLength > afterGCLength);
    }
}

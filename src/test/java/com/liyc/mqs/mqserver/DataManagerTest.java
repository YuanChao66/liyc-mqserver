package com.liyc.mqs.mqserver;

import com.liyc.mqs.MqsApplication;
import com.liyc.mqs.mqserver.core.Binding;
import com.liyc.mqs.mqserver.core.Exchange;
import com.liyc.mqs.mqserver.core.ExchangeType;
import com.liyc.mqs.mqserver.core.Queue;
import com.liyc.mqs.mqserver.datacenter.DataManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

/**
 * 单元测试类
 *  - 引入DataBaseManeager
 *  - 在每个操作之前setUp：给MqApplication.context赋值；dataManager.init。
 *  - 在每个操作之后teraDown：1.关闭contenxt 2.dataManager.close。
 *
 * @author Liyc
 * @date 2024/12/12 17:46
 **/

@SpringBootTest
public class DataManagerTest {
    private DataManager dataManager = new DataManager();

    // 接下来下面这里需要编写多个 方法 . 每个方法都是一个/一组单元测试用例.
    // 还需要做一个准备工作. 需要写两个方法, 分别用于进行 "准备工作" 和 "收尾工作"

    // 使用这个方法, 来执行准备工作. 每个用例执行前, 都要调用这个方法.
    @BeforeEach
    public void setUp(){
        // 由于在 init 中, 需要通过 context 对象拿到 metaMapper 实例的.
        // 所以就需要先把 context 对象给搞出来.
        MqsApplication.context = SpringApplication.run(MqsApplication.class);
        dataManager.init();
    }

    // 使用这个方法, 来执行收尾工作. 每个用例执行后, 都要调用这个方法.
    @AfterEach
    public void tearDown(){
        // 这里要进行的操作, 就是把数据库给清空~~ (把数据库文件, meta.db 直接删了就行了)
        // 注意, 此处不能直接就删除, 而需要先关闭上述 context 对象!!
        // 此处的 context 对象, 持有了 MetaMapper 的实例, MetaMapper 实例又打开了 meta.db 数据库文件.
        // 如果 meta.db 被别人打开了, 此时的删除文件操作是不会成功的 (Windows 系统的限制, Linux 则没这个问题).
        // 另一方面, 获取 context 操作, 会占用 8080 端口. 此处的 close 也是释放 8080.
        MqsApplication.context.close();
        dataManager.deleteDB();
    }

    @Test
    public void selectAll() {
        // 由于 init 方法, 已经在上面 setUp 中调用过了. 直接在测试用例代码中, 检查当前的数据库状态即可.
        // 直接从数据库中查询. 看数据是否符合预期.
        // 查交换机表, 里面应该有一个数据(匿名的 exchange); 查队列表, 没有数据; 查绑定表, 没有数据.
        List<Exchange> exchangeList = dataManager.selectAllExchanges();
        List<Queue> queueList = dataManager.selectAllQueues();
        List<Binding> bindingList = dataManager.selectAllBindings();

        // 直接打印结果, 通过肉眼来检查结果, 固然也可以. 但是不优雅, 不方便.
        // 更好的办法是使用断言.
        // System.out.println(exchangeList.size());
        // assertEquals 判定结果是不是相等.
        // 注意这俩参数的顺序. 虽然比较相等, 谁在前谁在后, 无所谓.
        // 但是 assertEquals 的形参, 第一个形参叫做 expected (预期的), 第二个形参叫做 actual (实际的)
        Assertions.assertEquals(1, exchangeList.size());
        Assertions.assertEquals(0, queueList.size());
        Assertions.assertEquals(0, bindingList.size());
        Assertions.assertEquals("insertExchange", exchangeList.get(0).getName());
        Assertions.assertEquals(ExchangeType.TOPIC, exchangeList.get(0).getType());

    }

    public Exchange createExchange(String exchangeName){
        Exchange exchange = new Exchange();
        exchange.setName(exchangeName);
        exchange.setType(ExchangeType.TOPIC);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        return exchange;
    }

    @Test
    public void insertExchange(){
        Exchange exchange = createExchange("insertExchange");
        dataManager.insertExchange(exchange);

        List<Exchange> exchangeList = dataManager.selectAllExchanges();
        Assertions.assertEquals(2,exchangeList.size());
        Exchange exchange1 = exchangeList.get(1);
        Assertions.assertEquals("insertExchange",exchange1.getName());
        Assertions.assertEquals(ExchangeType.TOPIC,exchange1.getType());
        Assertions.assertEquals(true,exchange1.isDurable());
        Assertions.assertEquals(false,exchange1.isAutoDelete());
    }

    @Test
    public void deleteExchange(){
        // 先构造一个交换机, 插入数据库; 然后再按照名字删除即可!
        Exchange exchange = createExchange("testExchange");
        dataManager.insertExchange(exchange);
        List<Exchange> exchangeList = dataManager.selectAllExchanges();
        Assertions.assertEquals(2, exchangeList.size());
        Assertions.assertEquals("testExchange", exchangeList.get(1).getName());

        // 进行删除操作
        dataManager.deleteExchange("testExchange");
        // 再次查询
        exchangeList = dataManager.selectAllExchanges();
        Assertions.assertEquals(1, exchangeList.size());
        Assertions.assertEquals("", exchangeList.get(0).getName());
    }

    private Queue createQueue(String name) {
        Queue queue = new Queue();
        queue.setName(name);
        queue.setDurable(true);
        queue.setAutoDelete(false);
        queue.setExclusive(false);
        queue.setArguments("aaa",1);
        queue.setArguments("bbb",2);
        return queue;
    }
    @Test
    void insertQueue() {
        Queue queue = createQueue("testQueue");
        int row = dataManager.insertQueue(queue);

        List<Queue> queues = dataManager.selectAllQueues();

        Assertions.assertEquals(1,row);
        Assertions.assertEquals(1,queues.size());
        Assertions.assertEquals("testQueue",queues.get(0).getName());
        Assertions.assertEquals(true,queues.get(0).isDurable());
        Assertions.assertEquals(false,queues.get(0).isAutoDelete());
        Assertions.assertEquals(false,queues.get(0).isExclusive());
        Assertions.assertEquals(1,queues.get(0).getArguments("aaa"));
        Assertions.assertEquals(2,queues.get(0).getArguments("bbb"));
    }

    @Test
    void deleteQueue() {
        Queue queue = createQueue("testQueue");
        int row = dataManager.insertQueue(queue);

        List<Queue> queues = dataManager.selectAllQueues();

        Assertions.assertEquals(1,row);
        Assertions.assertEquals(1,queues.size());
        Assertions.assertEquals("testQueue",queues.get(0).getName());
        Assertions.assertEquals(true,queues.get(0).isDurable());
        Assertions.assertEquals(false,queues.get(0).isAutoDelete());
        Assertions.assertEquals(false,queues.get(0).isExclusive());
        Assertions.assertEquals(1,queues.get(0).getArguments("aaa"));
        Assertions.assertEquals(2,queues.get(0).getArguments("bbb"));

        row = dataManager.deleteQueue("testQueue");
        queues = dataManager.selectAllQueues();
        Assertions.assertEquals(1,row);
        Assertions.assertEquals(0,queues.size());

    }

    private Binding createBinding(String exchangeName,String queueName,String bindingKey) {
        Binding binding = new Binding();
        binding.setQueueName(queueName);
        binding.setExchangeName(exchangeName);
        binding.setBindingKey(bindingKey);
        return binding;
    }

    @Test
    void insertBinding() {
        Binding binding = createBinding("testExchangeName","testQueueName","hello");

        int row = dataManager.insertBinding(binding);

        List<Binding> bindings = dataManager.selectAllBindings();

        Assertions.assertEquals(1,row);
        Assertions.assertEquals(1,bindings.size());
        Assertions.assertEquals("testExchangeName",bindings.get(0).getExchangeName());
        Assertions.assertEquals("testQueueName",bindings.get(0).getQueueName());
        Assertions.assertEquals("hello",bindings.get(0).getBindingKey());
    }

    @Test
    void deleteBinding() {
        Binding binding = createBinding("testExchangeName","testQueueName","hello");

        int row = dataManager.insertBinding(binding);

        List<Binding> bindings = dataManager.selectAllBindings();

        Assertions.assertEquals(1,row);
        Assertions.assertEquals(1,bindings.size());
        Assertions.assertEquals("testExchangeName",bindings.get(0).getExchangeName());
        Assertions.assertEquals("testQueueName",bindings.get(0).getQueueName());
        Assertions.assertEquals("hello",bindings.get(0).getBindingKey());

        row = dataManager.deleteBinding(binding);
        bindings = dataManager.selectAllBindings();

        Assertions.assertEquals(1,row);
        Assertions.assertEquals(0,bindings.size());

    }
}

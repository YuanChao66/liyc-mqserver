package com.liyc.mqs.mqserver.datacenter;

import com.liyc.mqs.MqsApplication;
import com.liyc.mqs.mqserver.core.Binding;
import com.liyc.mqs.mqserver.core.Exchange;
import com.liyc.mqs.mqserver.core.ExchangeType;
import com.liyc.mqs.mqserver.core.Queue;
import com.liyc.mqs.mqserver.mapper.MetaMapper;
import java.io.File;
import java.util.List;

/**
 * 数据库工厂方法
 * ①init（在MqApplication.context里获取mapper的bean；如果数据库目录不存在，则创建数据库目录，创建数据表，插入默认数据）
 * ②删除数据库目录（删除db文件 删除目录）
 * ③检查数据库目录是否存在
 * ④创建数据库表方法
 * ⑤创建一个默认数据插入到数据库种
 * ⑥针对队列、交换机、绑定创建插入，查询，删除方法。
 *
 * @author Liyc
 * @date 2024/12/12 16:30
 **/

public class DataManager {
    private MetaMapper metaMapper;

    // 针对数据库进行初始化
    public void init() {
            // 手动的获到 ObjectMapper
            metaMapper = MqsApplication.context.getBean(MetaMapper.class);

        if (!checkDBExists()) {
            // 数据库不存在, 就进行建建库表操作
            // 先创建一个 data 目录
            File dataDir = new File("./data");
            dataDir.mkdirs();
            // 创建数据表
            createTable();
            // 插入默认数据
            createDefaultData();
            System.out.println("[DataBaseManager] 数据库初始化完成!");
        } else {
                // 数据库已经存在了, 啥都不必做即可
                System.out.println("[DataBaseManager] 数据库已经存在!");
        }
    }

    public void deleteDB() {
            File file = new File("./data/meta.db");
        boolean ret = file.delete();
        if (ret) {
                System.out.println("[DataBaseManager] 删除数据库文件成功!");
        } else {
                System.out.println("[DataBaseManager] 删除数据库文件失败!");
        }

        File dataDir = new File("./data");
        // 使用 delete 删除目录的时候, 需要保证目录是空的.
        ret = dataDir.delete();
        if (ret) {
                System.out.println("[DataBaseManager] 删除数据库目录成功!");
        } else {
                System.out.println("[DataBaseManager] 删除数据库目录失败!");
        }
    }

    private boolean checkDBExists() {
            File file = new File("./data/meta.db");
        if (file.exists()) {
            return true;
        }
        return false;
    }

    // 这个方法用来建表.
    // 建库操作并不需要手动执行. (不需要手动创建 meta.db 文件)
    // 首次执行这里的数据库操作的时候, 就会自动的创建出 meta.db 文件来 (MyBatis 帮我们完成的)
    private void createTable() {
        metaMapper.createExchangeTable();
        metaMapper.createQueueTable();
        metaMapper.createBindingTable();
        System.out.println("[DataBaseManager] 创建表完成!");
    }

    // 给数据库表中, 添加默认的数据.
    // 此处主要是添加一个默认的交换机.
    // RabbitMQ 里有一个这样的设定: 带有一个 匿名 的交换机, 类型是 DIRECT.
    private void createDefaultData() {
            // 构造一个默认的交换机.
            Exchange exchange = new Exchange();
        exchange.setName("");
        exchange.setType(ExchangeType.DIRECT);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        metaMapper.insertExchange(exchange);
        System.out.println("[DataBaseManager] 创建初始数据完成!");
    }

    // 把其他的数据库的操作, 也在这个类中封装一下.
    public int insertExchange(Exchange exchange) {
        return metaMapper.insertExchange(exchange);
    }

    public List<Exchange> selectAllExchanges() {
        return metaMapper.selectAllExchanges();
    }

    public int deleteExchange(String exchangeName) {
        return metaMapper.deleteExchange(exchangeName);
    }

    public int insertQueue(Queue queue) {
        return metaMapper.insertQueue(queue);
    }

    public List<Queue> selectAllQueues() {
        return metaMapper.selectAllQueues();
    }

    public int deleteQueue(String queueName) {
        return metaMapper.deleteQueue(queueName);
    }

    public int insertBinding(Binding binding) {
        return metaMapper.insertBinding(binding);
    }

    public List<Binding> selectAllBindings() {
        return metaMapper.selectAllBindings();
    }

    public int deleteBinding(Binding binding) {
            return metaMapper.deleteBinding(binding);
    }
}

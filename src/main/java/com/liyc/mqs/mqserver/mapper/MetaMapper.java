package com.liyc.mqs.mqserver.mapper;

import com.liyc.mqs.mqserver.core.Binding;
import com.liyc.mqs.mqserver.core.Exchange;
import com.liyc.mqs.mqserver.core.Queue;
import org.apache.ibatis.annotations.Mapper;
import java.util.List;

@Mapper
public interface MetaMapper {
    void createExchangeTable();
    void createQueueTable();
    void createBindingTable();

    List<Exchange> selectAllExchanges();
    List<Queue> selectAllQueues();
    List<Binding> selectAllBindings();

    int insertExchange(Exchange exchange);
    int deleteExchange(String exchangeName);
    int insertQueue(Queue msgQueue);
    int deleteQueue(String queueName);
    int insertBinding(Binding binding);
    int deleteBinding(Binding binding);

}

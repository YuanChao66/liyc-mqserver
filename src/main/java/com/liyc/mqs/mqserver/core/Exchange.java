package com.liyc.mqs.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 交换机基础类
 *
 * @author Liyc
 * @date 2024/12/12 10:53
 **/

public class Exchange {
    //交换机名称
    private String name;
    //交换机类型
    private ExchangeType type = ExchangeType.DIRECT;
    //是否持久化
    private boolean durable;
    //是否自动删除
    private boolean autoDelete;
    //其他参数
    // arguments 表示创建交换机时的一些额外的参数。
    // 为了实现 arguments 在数据库中的存取, 就需要把 Map 转成 json 格式的字符串，并对JSON字符串进行解析.
    // 因此需要调整一下 argument 的 get 和 set 方法.
    private Map<String, Object> arguments = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExchangeType getType() {
        return type;
    }

    public void setType(ExchangeType type) {
        this.type = type;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    // 这里的 get set 用于和数据库交互使用.
    // 这个方法,在将数据存到数据库时,会自动调用到，将Map对象转换成JSON字符串
    public String getArguments() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String arg = objectMapper.writeValueAsString(this.arguments);
            return arg;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "{}";
    }

    // 这个方法, 是从数据库读到数据之后, 构造 Exchange 对象, 会自动调用到，将JSON字符串转换成Map对象
    public void setArguments(String arguments) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            // 把参数中的 argumentsJson 按照 JSON 格式解析, 转成 Map 对象
            this.arguments = objectMapper.readValue(arguments, new TypeReference<HashMap<String,Object>>() {});
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
    // 提供一组 getter setter , 用来更方便的获取/设置这里的键值对.
    // 这一组在 java 代码内部使用 (比如测试的时候)
    public Object getArguments(String key){
        return arguments.get(key);
    }
    public void setArguments(String key, Object value) {
        this.arguments.put(key, value);
    }
    public void setArguments(HashMap<String,Object> arguments) {
        this.arguments = arguments;
    }
}

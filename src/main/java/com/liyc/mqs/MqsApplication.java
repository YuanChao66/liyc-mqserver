package com.liyc.mqs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * 启动类
 *
 * @author Liyc
 * @date 2024/12/12 10:52
 **/
@SpringBootApplication
public class MqsApplication {
    // 获取Spring上下文对象
    public static ConfigurableApplicationContext context;

    public MqsApplication() {
    }

    public static void main(String[] args) {
        context = SpringApplication.run(MqsApplication.class, args);
    }
}

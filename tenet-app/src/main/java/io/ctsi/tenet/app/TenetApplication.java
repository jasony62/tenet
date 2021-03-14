package io.ctsi.tenet.app;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;

/**
 * 容器启动入口tenet-app负责打包管理依赖
 * 项目的配置文件xxx.properties主要放入tenet-app ./config中
 * 主要逻辑放入tenet-core.jar中
 * 依赖jar主要放入./libx中
 * 功能：
 * 1、自动加载依赖plugin中的autoconfig
 * 2、启动守护线程池
 */
@SpringBootApplication(exclude = {KafkaAutoConfiguration.class, MongoAutoConfiguration.class})
@Slf4j
public class TenetApplication {

    public static void main(String[] args)  {
        SpringApplication.run(TenetApplication.class);
        log.info("+++ tenet 启动 +++");
    }

}

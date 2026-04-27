package com.kpdp.config;

import cn.hutool.core.util.StrUtil;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Redisson 配置，直接复用 Spring Redis 连接参数。
 */
@Configuration
public class RedissonConfig {

    /**
     * 与 Spring Data Redis 共用 Boot 3.x 官方配置前缀，避免两个客户端读到不同参数。
     */
    @Value("${spring.data.redis.host}")
    private String host;

    @Value("${spring.data.redis.port}")
    private int port;

    @Value("${spring.data.redis.password:}")
    private String password;

    @Bean
    public RedissonClient redissonClient() {
        Config config = new Config();
        SingleServerConfig serverConfig = config.useSingleServer()
                .setAddress(String.format("redis://%s:%d", host, port));
        if (StrUtil.isNotBlank(password)) {
            serverConfig.setPassword(password);
        }
        return Redisson.create(config);
    }
}

package com.kpdp;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@MapperScan("com.kpdp.mapper")
@SpringBootApplication
@EnableScheduling
public class KpDianPingApplication {

    public static void main(String[] args) {
        SpringApplication.run(KpDianPingApplication.class, args);
    }
}

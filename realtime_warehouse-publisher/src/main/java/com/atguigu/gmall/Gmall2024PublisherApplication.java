package com.atguigu.gmall;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall.mapper")
public class Gmall2024PublisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(Gmall2024PublisherApplication.class, args);
    }
}
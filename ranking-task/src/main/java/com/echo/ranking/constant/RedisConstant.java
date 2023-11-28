package com.echo.ranking.constant;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * redis常量
 */
@Slf4j
public class RedisConstant {

    /**
     * host
     */
    public static String HOST;

    /**
     * port
     */
    public static Integer PORT;

    /**
     * timeout
     */
    public static Integer TIMEOUT;

    /**
     * password
     */
    public static String PASSWORD;

    /**
     * database
     */
    public static Integer DATABASE;

    static {
        Properties prop = new Properties();
        InputStream input = null;
        try {
            input = RedisConstant.class.getClassLoader().getResourceAsStream("redis.properties");
            // 加载配置文件
            prop.load(input);
            // 读取配置
            HOST = prop.getProperty("redis.host");
            PORT = Integer.parseInt(prop.getProperty("redis.port"));
            TIMEOUT = Integer.parseInt(prop.getProperty("redis.timeout"));
            PASSWORD = prop.getProperty("redis.password");
            DATABASE = Integer.parseInt(prop.getProperty("redis.database"));
            log.info("redis config host:{} port:{} timeout:{} password:{} database:{}", HOST, PORT, TIMEOUT, PASSWORD, DATABASE);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }


}

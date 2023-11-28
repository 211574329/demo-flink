package com.echo.ranking.constant;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * rabbit常量
 */
@Slf4j
public class RabbitConstant {

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
    public static String USERNAME;

    /**
     * password
     */
    public static String PASSWORD;

    /**
     * database
     */
    public static String VIRTUALHOST;

    static {
        Properties prop = new Properties();
        InputStream input = null;
        try {
            input = RabbitConstant.class.getClassLoader().getResourceAsStream("rabbit.properties");
            // 加载配置文件
            prop.load(input);
            // 读取配置
            HOST = prop.getProperty("rabbit.host");
            PORT = Integer.parseInt(prop.getProperty("rabbit.port"));
            USERNAME = prop.getProperty("rabbit.username");
            PASSWORD = prop.getProperty("rabbit.password");
            VIRTUALHOST = prop.getProperty("rabbit.virtualhost");
            log.info("rabbit config host:{} port:{} username:{} password:{} virtualhost:{}", HOST, PORT, USERNAME, PASSWORD, VIRTUALHOST);
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

package com.hadoop.demo.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @author fujh
 * @since 2020年10月20日20:16:52
 * HDFS配置类
 */
@Configuration
public class HdfsConfig {

    /**
     * HDFS地址
     */
    @Value("${hdfs.path}")
    private String path;

    /**
     * HDFS启动用户
     */
    @Value("${hdfs.user}")
    private String user;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
}

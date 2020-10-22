package com.hadoop.demo.config;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

/**
 * @author fujh
 * @since 2020年10月20日20:16:52
 * HDFS配置类
 *  配置HDFS核心的两个类：
 *      1、org.apache.hadoop.conf.Configuration  ==》 配置类
 *      2、org.apache.hadoop.fs.FileSystem  ==》 操作类
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

    /**
     * 副本系数
     */
    @Value("${hdfs.replication}")
    private String replication;

    /**
     * HDFS配置类
     * @return
     */
    public org.apache.hadoop.conf.Configuration getConfiguration(){
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        /**
         * 配置HDFS地址
         */
        configuration.set("fs.defaultFS",path);
        /**
         * 临时存储目录（配置的还是服务器上的地址）
         */
        configuration.set("hadoop.tmp.dir","/root/software/hadoop-2.6.0/tmp");
        /**
         * 副本系数为1
         */
        configuration.set("dfs.replication",replication);
        System.out.println("--------------------------------------------------------------------------------------");
        System.out.println("读取HDFS配置信息fs.defaultFS:"+configuration.get("fs.defaultFS"));
        System.out.println("读取HDFS配置信息hadoop.tmp.dir:"+configuration.get("hadoop.tmp.dir"));
        System.out.println("读取HDFS配置信息dfs.replication:"+configuration.get("dfs.replication"));
        System.out.println("--------------------------------------------------------------------------------------");
        return configuration;
    }

    /**
     * 利用IOC容器管控FileSystem实例
     * @return
     */
    @Bean
    public FileSystem getFileSystem(){
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(path), getConfiguration(), user);
        }catch (Exception e){
            System.out.println("FileSystem初始化失败！");
            e.printStackTrace();
        }
        return fs;
    }

}

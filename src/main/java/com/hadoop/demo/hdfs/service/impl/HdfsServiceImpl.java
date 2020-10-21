package com.hadoop.demo.hdfs.service.impl;

import com.hadoop.demo.hdfs.service.HdfsService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author fujh
 * @since 2020年10月21日10:52:46
 * 操作HDFS业务实现类：
 *  实现操作HDFS核心的两个类：
 *      1、org.apache.hadoop.conf.Configuration  ==》 配置类
 *      2、org.apache.hadoop.fs.FileSystem  ==》 操作类
 */
@Service
public class HdfsServiceImpl implements HdfsService {

    /**
     * HDFS地址
     */
    @Value("${hdfs.path}")
    private static String path;

    /**
     * HDFS操作用户
     */
    @Value("${hdfs.user}")
    private static String user;

    /**
     * 配置HDFS配置类
     * @return
     */
    private static Configuration getConfiguration(){
        Configuration configuration = new Configuration();
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
        configuration.set("dfs.replication","1");
        return configuration;
    }

    /**
     * 配置HDFS文件系统类FileSystem
     * @return
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    private static FileSystem getFileSystem() throws URISyntaxException, IOException, InterruptedException {
        /**
         * 配置HDFS地址，配置类，操作用户
         */
        return FileSystem.get(new URI(path),getConfiguration(),user);
    }

    @Override
    public String showInfo() {
        return path+"---"+user;
    }
}

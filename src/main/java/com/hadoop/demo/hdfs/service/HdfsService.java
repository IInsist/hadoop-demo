package com.hadoop.demo.hdfs.service;

import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * @author fujh
 * @sine 2020年10月20日20:28:08
 * HDFS业务接口
 */
public interface HdfsService {

    /**
     * 测试
     * @return
     */
    String showInfo();

    /**
     * 查看文件
     * @param path
     * @return
     */
    List<Path> lsitFiles(String path) throws Exception;

    /**
     * 创建目录
     * @param path
     */
    boolean mkdir(String path) throws Exception;

    /**
     * 删除指定目录
     * @param path
     * @return
     * @throws Exception
     */
    boolean delDir(String path) throws Exception;

    /**
     * 删除指定目录文件
     * @param filePath
     * @return
     * @throws Exception
     */
    boolean delFile(String filePath) throws Exception;


}

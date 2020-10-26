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
     * 查看执行目录下文件
     * @param path
     * @return
     */
    List<Path> listPaths(String path);

    /**
     * 创建目录
     * @param path
     */
    boolean mkdir(String path);

    /**
     * 删除指定目录
     * @param path
     * @return
     * @throws Exception
     */
    boolean delDir(String path);

    /**
     * 删除指定目录文件
     * @param filePath
     * @return
     * @throws Exception
     */
    boolean delFile(String filePath);

    /**
     * 上传文件
     * @param filePath
     * @param targetPath
     * @return
     * @throws Exception
     */
    boolean uploadFile(String filePath,String targetPath);

    /**
     * 文件下载
     * @param filePath
     * @param filePath
     * @return
     * @throws Exception
     */
    boolean downloadFile(String filePath,String destPath);

    /**
     * 移动文件
     * @param filePath
     * @param destPath
     * @return
     * @throws Exception
     */
    boolean moveFile(String filePath,String destPath);

    /**
     * 文件重命名
     * @param filePath
     * @param newName
     * @return
     * @throws Exception
     */
    boolean renameFile(String filePath,String newName);
}

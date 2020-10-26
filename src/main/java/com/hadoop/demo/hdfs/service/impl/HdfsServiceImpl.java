package com.hadoop.demo.hdfs.service.impl;

import com.hadoop.demo.hdfs.service.HdfsService;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.fs.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * @author fujh
 * @since 2020年10月21日10:52:46

 */
@Service
@Log4j2
public class HdfsServiceImpl implements HdfsService {

    @Autowired
    private FileSystem fileSystem;

    @Override
    public String showInfo() {
        return fileSystem.getUri().getPath();
    }

    /**
     * 查看文件
     *
     * @param path
     * @return
     */
    @Override
    public List<Path> lsitPaths(String path) throws Exception {
        List<Path> result = new ArrayList<>();
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
        System.out.println("路径输出开始----------------------------------------");
        for (int i = 0; i < fileStatuses.length; i++) {
            Path temp = fileStatuses[i].getPath();
            System.out.println("temp:"+temp.toString());
            result.add(temp);
        }
        System.out.println("路径输出结束----------------------------------------");
        return result;
    }

    /**
     * 创建目录
     *
     * @param path
     */
    @Override
    public boolean mkdir(String path) throws Exception {
        if(!fileSystem.exists(new Path(path))){
            return fileSystem.mkdirs(new Path(path));
        }
        return true;
    }

    /**
     * 删除指定目录
     *
     * @param path
     * @return
     * @throws Exception
     */
    @Override
    public boolean delDir(String path) throws Exception {
        if(!fileSystem.exists(new Path(path))){
            return false;
        }
        return fileSystem.delete(new Path(path),false);
    }

    /**
     * 删除指定目录文件
     *
     * @param filePath
     * @return
     * @throws Exception
     */
    @Override
    public boolean delFile(String filePath) throws Exception {
        if(!fileSystem.exists(new Path(filePath))){
            return false;
        }
        return fileSystem.delete(new Path(filePath),true);
    }

    /**
     * 上传文件
     *
     * @param filePath
     * @param targetPath
     * @return
     * @throws Exception
     */
    @Override
    public boolean uploadFile(String filePath, String targetPath) throws Exception {
        try {
            Path target = new Path(targetPath);
            Path file = new Path(filePath);
            fileSystem.copyFromLocalFile(file,target);
        }catch (Exception e){
            log.error("复制文件异常",e);
            return false;
        }
        return true;
    }
}

package com.hadoop.demo.hdfs.service.impl;

import com.hadoop.demo.hdfs.service.HdfsService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @author fujh
 * @since 2020年10月21日10:52:46

 */
@Service
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
    public List<Path> lsitFiles(String path) throws Exception {
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
        return fileSystem.mkdirs(new Path(path));
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
        return fileSystem.delete(new Path(filePath),true);
    }
}

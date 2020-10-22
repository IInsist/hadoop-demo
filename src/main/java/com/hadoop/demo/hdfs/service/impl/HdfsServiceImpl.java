package com.hadoop.demo.hdfs.service.impl;

import com.hadoop.demo.hdfs.service.HdfsService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path(path), false);
        List<Path> paths = new ArrayList<>();
        System.out.println("HDFS路径输出开始------------------------------------------");
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            paths.add(next.getPath());
            System.out.println(next.getPath().toString());
        }
        System.out.println("HDFS路径输出结束------------------------------------------");
        return paths;
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
}

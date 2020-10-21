package com.hadoop.demo.hdfs.service.impl;

import com.hadoop.demo.hdfs.service.HdfsService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class HdfsServiceImpl implements HdfsService {

    @Value("${hdfs.path}")
    private String path;

    @Value("${hdfs.user}")
    private String user;

    @Override
    public String showInfo() {
        return path+"---"+user;
    }

}

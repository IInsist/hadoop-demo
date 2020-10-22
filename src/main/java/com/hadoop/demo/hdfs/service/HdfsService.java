package com.hadoop.demo.hdfs.service;

import com.hadoop.demo.config.HdfsConfig;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.net.URISyntaxException;
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
    List<Object> lsitFiles(String path) throws Exception;
}

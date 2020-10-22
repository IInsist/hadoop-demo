package com.hadoop.demo.hdfs;

import com.hadoop.demo.hdfs.service.HdfsService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

/**
 * @author
 * @since 2020年10月21日11:11:40
 * HDFS测试类
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class HdfsTest {

    @Resource
    private HdfsService service;

    @Test
    public void test(){
    }

}

package com.hadoop.demo.hdfs;

import com.hadoop.demo.hdfs.service.HdfsService;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.List;

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

    /**
     * 查看指定目录下子目录
     * @throws Exception
     */
    @Test
    public void lsitPaths() throws Exception {
        List<Path> paths = service.lsitPaths("/");
        System.out.println("paths:"+paths.toString());
    }

    /**
     * 创建目录
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception {
        service.mkdir("/test");
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void delFile() throws Exception {
        if(service.delFile("/test/Linux服务列表.pdf")){
            System.out.println("删除成功！");
        }
    }

    /**
     * 上传文件
     * @throws Exception
     */
    @Test
    public void uploadFile() throws Exception {
        String filePath = "E:\\Linux服务列表.pdf";
        String targetPath = "/test";
        if(service.uploadFile(filePath,targetPath)){
            System.out.println("上传成功！");
        }
    }
}

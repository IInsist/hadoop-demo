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
 * @author fujh
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
     */
    @Test
    public void listPaths(){
        List<Path> paths = service.listPaths("/user/root/");
        System.out.println("paths:"+paths.toString());
    }

    /**
     * 创建目录
     */
    @Test
    public void mkdir(){
        service.mkdir("/test/output/");
    }

    /**
     * 删除文件
     */
    @Test
    public void delFile(){
        if(service.delFile("/test/input/input/student_info.txt")){
            System.out.println("删除成功！");
        }
    }

    /**
     * 删除目录
     */
    @Test
    public void delDir(){
        if(service.delDir("/test/input/input")){
            System.out.println("删除成功！");
        }
    }

    /**
     * 上传文件
     */
    @Test
    public void uploadFile(){
        //String filePath = "E:\\Linux服务列表.pdf";
        String filePath = "F:\\WORKSPACR\\TempWORKSPACE\\hadoop-demo\\data\\input\\student_info.txt";
        String targetPath = "/test/input";
        if(service.uploadFile(filePath,targetPath)){
            System.out.println("上传成功！");
        }
    }

    /**
     * 文件下载
     */
    @Test
    public void downloadFile(){
        //String filePath = "/data/xy_bjsl.txt";
        String filePath = "/test/Linux服务列表.pdf";
        String targetPath = "D:\\fiels\\";
        if(service.downloadFile(filePath,targetPath)){
            System.out.println("下载成功！");
        }
    }

    /**
     * 复制文件
     */
    @Test
    public void copyFile(){
        String filePath = "/test/Linux服务列表.pdf";
        String targetPath = "/data/Linux服务列表.pdf";
        if(service.copyFile(filePath,targetPath)){
            System.out.println("复制成功！");
        }
    }

    /**
     * 移动文件
     */
    @Test
    public void moveFile(){
        String filePath = "/test/Linux服务列表.pdf";
        String targetPath = "/data/";
        if(service.moveFile(filePath,targetPath)){
            System.out.println("移动文件成功！");
        }
    }

    /**
     * 读取文件内容
     */
    @Test
    public void readTxtFile(){
        String filePath = "/data/teacher_info.txt";
        StringBuffer stringBuffer = service.readTxtFile(filePath);
    }
}

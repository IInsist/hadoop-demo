package com.hadoop.demo.hdfs.service.impl;

import com.hadoop.demo.hdfs.service.HdfsService;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author fujh
 * @since 2020年10月21日10:52:46
 * HDFS业务实现类
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
     * 查看指定目录下文件
     *
     * @param path
     * @return
     */
    @Override
    public List<Path> listPaths(String path){
        List<Path> result = new ArrayList<>();
        try {
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
            System.out.println("路径输出开始----------------------------------------");
            for (int i = 0; i < fileStatuses.length; i++) {
                Path temp = fileStatuses[i].getPath();
                System.out.println("temp:"+temp.toString());
                result.add(temp);
            }
            System.out.println("路径输出结束----------------------------------------");
            return result;
        }catch (Exception e){
            log.error("查看指定目录下文件异常！",e);
            return result;
        }
    }

    /**
     * 创建目录
     *
     * @param path
     */
    @Override
    public boolean mkdir(String path){
        try {
            Path obj = new Path(path);
            if(!fileSystem.exists(obj)){
                return fileSystem.mkdirs(obj);
            }
            return true;
        }catch (Exception e){
            log.error("创建目录异常！",e);
            return false;
        }
    }

    /**
     * 删除指定目录
     *
     * @param path
     * @return
     */
    @Override
    public boolean delDir(String path){
        try {
            Path obj = new Path(path);
            if(!fileSystem.exists(obj)){
                System.out.println("目录不存在："+path);
                return false;
            }
            return fileSystem.delete(obj,false);
        }catch (Exception e){
            log.error("删除指定目录异常！",e);
            return false;
        }
    }

    /**
     * 删除指定目录文件
     *
     * @param filePath
     * @return
     */
    @Override
    public boolean delFile(String filePath){
        try {
            if(!fileSystem.exists(new Path(filePath))){
                return false;
            }
            return fileSystem.delete(new Path(filePath),true);
        }catch (Exception e){
            log.error("删除指定目录文件异常！",e);
            return false;
        }
    }

    /**
     * 上传文件
     *
     * @param filePath
     * @param targetPath
     * @return
     */
    @Override
    public boolean uploadFile(String filePath, String targetPath){
        try {
            Path target = new Path(targetPath);
            if(fileSystem.isDirectory(target)){
                Path file = new Path(filePath);
                fileSystem.copyFromLocalFile(file,target);
            }
            return true;
        }catch (Exception e){
            log.error("复制文件异常",e);
            return false;
        }
    }

    /**
     * 文件下载
     *
     * @param filePath
     * @param destPath
     * @return
     */
    @Override
    public boolean downloadFile(String filePath,String destPath){
        try {
            Path file = new Path(filePath);
            if(fileSystem.exists(file)){
                Path dest = new Path(destPath);
                fileSystem.copyToLocalFile(file,dest);
            }
            return true;
        }catch (Exception e){
            log.error("文件下载异常！",e);
            return false;
        }
    }

    /**
     * 移动文件
     *
     * @param filePath
     * @param destPath
     * @return
     */
    @Override
    public boolean moveFile(String filePath, String destPath){
        try {
            Path file = new Path(filePath);
            if(!fileSystem.exists(file)){
                return false;
            }
            //获得文件信息
            FileStatus fileStatus = fileSystem.getFileStatus(file);
            Path dest = new Path(destPath+"/"+fileStatus.getPath().getName());

            System.out.println("移动的目标文件："+dest.toUri().getPath());
            //复制文件
            FSDataInputStream in = null;
            FSDataOutputStream out = null;
            in = fileSystem.open(file);
            out = fileSystem.create(dest,false);
            IOUtils.copyBytes(in,out,1024);
            in.close();
            out.close();

            //删除源文件
            delFile(filePath);
            return true;
        }catch (Exception e){
            log.error("移动文件异常！",e);
            return false;
        }
    }

    /**
     * 复制文件
     *
     * @param filePath
     * @param destPath
     * @return
     */
    @Override
    public boolean copyFile(String filePath, String destPath) {
        try {
            Path file = new Path(filePath);
            if(fileSystem.exists(file)){
                Path dest = new Path(destPath);

                FSDataInputStream in = null;
                FSDataOutputStream out = null;
                in = fileSystem.open(file);
                out = fileSystem.create(dest,false);
                IOUtils.copyBytes(in,out,1024);
                in.close();
                out.close();
                return true;
            }
            return false;
        }catch (Exception e){
            log.error("复制文件异常！",e);
            return false;
        }
    }

    /**
     * 文件重命名
     *
     * @param filePath
     * @param newName
     * @return
     
     */
    @Override
    public boolean renameFile(String filePath, String newName) {
        try {
            Path file = new Path(filePath);
            if(fileSystem.exists(file)){
                fileSystem.rename(file,new Path(file.getParent()+"/"+newName));
            }
            return true;
        }catch (Exception e){
            log.error("移动文件异常！",e);
            return false;
        }
    }

    /**
     * 判断文件是否存在
     *
     * @param filePath
     * @return
     */
    @Override
    public boolean existsFile(String filePath) {
        try {
            if(StringUtils.isNotEmpty(filePath)){
                return fileSystem.exists(new Path(filePath));
            }
            return false;
        }catch (Exception e){
            log.error("判断文件是否存在异常！",e);
            return false;
        }
    }

    /**
     * 读取TXT类型文件
     *
     * @param filePath
     * @return
     */
    @Override
    public StringBuffer readTxtFile(String filePath) {
        try {
            if(StringUtils.isNotEmpty(filePath)){
                Path file = new Path(filePath);
                if(fileSystem.exists(file) && fileSystem.isFile(file)){
                    StringBuffer fileContent = new StringBuffer();
                    //文件内容
                    FSDataInputStream in = fileSystem.open(file);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in.getWrappedStream()));

                    System.out.println("读取文件："+filePath);
                    while (reader.readLine() != null){
                        String s = reader.readLine();
                        System.out.println("输出内容："+s);
                        fileContent.append(s);
                    }
                    in.close();
                    return fileContent;
                }
            }
            return null;
        }catch (Exception e){
            log.error("读取TXT类型文件异常！",e);
            return null;
        }
    }

    /**
     * 查看FileSystem对象初始化信息
     */
    @PostConstruct
    public void outputHdfsInfo(){
        System.out.println("HDFS相关信息：------------------------------");
        System.out.println("URI:"+fileSystem.getUri());
        System.out.println("HomeDirectory:"+fileSystem.getHomeDirectory());
        System.out.println("Scheme:"+fileSystem.getScheme());
        System.out.println("HDFS相关信息：------------------------------");
    }
}

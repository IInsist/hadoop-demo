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
     * @throws Exception
     */
    @Override
    public boolean delDir(String path){
        try {
            Path obj = new Path(path);
            if(!fileSystem.exists(obj)){
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
     * @throws Exception
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
     * @throws Exception
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
     * @throws Exception
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
     * @throws Exception
     */
    @Override
    public boolean moveFile(String filePath, String destPath){
        try {
            Path file = new Path(filePath);
            if(fileSystem.exists(file)){
                Path dest = new Path(destPath);
                if(fileSystem.isDirectory(dest)){
                    fileSystem.moveFromLocalFile(file,dest);
                }
            }
            return true;
        }catch (Exception e){
            log.error("移动文件异常！",e);
            return false;
        }
    }

    /**
     * 文件重命名
     *
     * @param filePath
     * @param newName
     * @return
     * @throws Exception
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
}

package com.hadoop.demo.hdfs.controller;

import com.hadoop.demo.hdfs.service.HdfsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author fujh
 * @since 2020年10月20日20:25:38
 * HDFS控制类
 */
@RestController
@RequestMapping("/hdfs")
public class HdfsController {

    @Resource
    private HdfsService service;

    @GetMapping("/show")
    public String showInfo(){
        return service.showInfo();
    }

}

package com.hadoop.demo.mr.studentInfo.gradeCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @author fujh
 * @since 2020年10月28日10:11:59
 * 学生信息之年级统计任务
 *  MapReduce Job核心类： org.apache.hadoop.mapreduce.Job
 *      1、配置Job执行的Mapper类
 *      2、配置Job执行的Reduce类
 *      3、配置读取、输出结果的路径
 *      4、执行Job
 */
@Component
public class GradeJob {

    /**
     * 对学生信息的年级进行统计
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //因为HDFS在服务器上，开发在windows上，所以需设定访问用户
        System.setProperty("HADOOP_USER_NAME","root");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://121.37.177.118");
        configuration.set("hadoop.tmp.dir","/root/software/hadoop-2.6.0/tmp");
        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("dfs.replication","1");

        Path input = new Path("/test/input/student_info.txt");
        Path output = new Path("/test/output/student_info/grade_count");

        /**
         * 定义Job类
         */
        Job greadCountJob = Job.getInstance(configuration, "学生信息之年级统计任务");

        //设置jar运行MapReduce类，目的为可通过 hadoop jar 的方式进行执行MepReduce
        greadCountJob.setJarByClass(GradeJob.class);

        /**
         * 1、设置map类
         */
        greadCountJob.setMapperClass(GradeMapper.class);
        greadCountJob.setMapOutputKeyClass(Text.class);
        greadCountJob.setMapOutputValueClass(IntWritable.class);

        /**
         * 2、设置reduce类
         */
        greadCountJob.setReducerClass(GradeReduce.class);
        greadCountJob.setOutputKeyClass(Text.class);
        greadCountJob.setOutputValueClass(IntWritable.class);

        /**
         * 3、设置读取文件、输出文件路径
         */
        FileInputFormat.addInputPath(greadCountJob,input);
        FileOutputFormat.setOutputPath(greadCountJob,output);

        /**
         * 4、启动任务，并且输出日志
         */
        boolean result = greadCountJob.waitForCompletion(true);

        //终止当前运行的JVM，0为正常终止，非零为异常终止
        System.exit(result == true ? 0 : 1);
    }

}

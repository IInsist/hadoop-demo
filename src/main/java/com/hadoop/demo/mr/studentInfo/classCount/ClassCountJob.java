package com.hadoop.demo.mr.studentInfo.classCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author fujh
 * @since 2020年10月28日19:22:27
 * 班级信息分析进行统计
 */
public class ClassCountJob {

    /**
     * 班级信息统计MapReduce任务
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //设置访问HDFS用户
        System.setProperty("HADOOP_USER_NAME","root");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://121.37.177.118");
        configuration.set("hadoop.tmp.dir","/root/software/hadoop-2.6.0/tmp");
        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("dfs.replication","1");

        //1、创建MapReduce任务类
        Job job = Job.getInstance(configuration, "学生信息之班级统计任务");

        //2、设置Mapper类
        job.setMapperClass(ClassMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //3、设置Reducer类
        job.setReducerClass(ClassReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //4、设置输入、输出数据路径
        Path inputPath = new Path("/test/input/student_info.txt");
        Path outputPath = new Path("/test/output/student_info/class_count/");
        FileInputFormat.addInputPath(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        //5、执行
        boolean result = job.waitForCompletion(true);
        System.exit(result==true ? 0 : 1);
        if(result){
            System.out.println("执行成功！");
        }else{
            System.out.println("执行成功！");
        }
    }

    /**
     * 班级信息统计Mapper
     */
    static class ClassMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        /**
         * Called once for each key/value pair in the input split. Most applications
         * should override this, but the default is the identity function.
         *
         * @param key 每行数据的偏移量（每行数据的位置）
         * @param value 每行数据的具体内容
         * @param context
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(value.getLength()==0){
                return;
            }
            String[] allInfo = value.toString().split("\t");
            System.out.println("xm:"+allInfo[1]+"---- bj:"+allInfo[5]);
            context.write(new Text(allInfo[5]),new IntWritable(1));
        }
    }

    /**
     * 班级信息统计Reduce类
     */
    static class ClassReduce extends Reducer<Text, IntWritable,Text, IntWritable>{
        /**
         * This method is called once for each key. Most applications will define
         * their reduce class by overriding this method. The default implementation
         * is an identity function.
         *
         * @param key
         * @param values
         * @param context
         *
         * map的输出到Reduce端，是按照相同key分发到一个reduce上去执行
         * reduce1: (谢某某,1) (谢某某,1) (谢某某,1)    ==> (谢某某,<1,1,1>)
         * reduce2: (张某某,1) (张某某,1)              ==> (张某某,<1,1>)
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> iterator = values.iterator();
            int count = 0;
            while (iterator.hasNext()){
                IntWritable next = iterator.next();
                //统计班级出现的次数
                count += next.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

}

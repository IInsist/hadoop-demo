package com.hadoop.demo.mr.studentInfo.majorCount;

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
 * @since 2020年10月28日15:52:32
 * 学生信息之专业统计
 */
public class MajorCountJob {

    /**
     * 学生信息的专业统计
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //设置执行用户
        System.setProperty("HADOOP_USER_NAME","root");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://121.37.177.118");
        configuration.set("hadoop.tmp.dir","/root/software/hadoop-2.6.0/tmp");
        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("dfs.replication","1");

        Job job = Job.getInstance(configuration, "学生信息之专业统计");
        job.setJarByClass(MajorCountJob.class);

        //1、设置Mapper
        job.setMapperClass(MajorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //2、设置Reducer
        job.setReducerClass(MajorReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //3、设置读取、输出文件路径
        Path inputPath = new Path("/test/input/student_info.txt");
        Path outputPath = new Path("/test/output/student_info/major_count/");

        FileInputFormat.addInputPath(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        //4、执行
        boolean result = job.waitForCompletion(true);
        System.exit(result==true ? 0 : 1);
        if(result){
            System.out.println("执行成功！");
        }else{
            System.out.println("执行失败！");
        }
    }

    /**
     * 统计专业的Mapper类
     */
    static class MajorMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        /**
         * Called once for each key/value pair in the input split. Most applications
         * should override this, but the default is the identity function.
         *
         * @param key 每行数据的偏移量（每行数据的位置）
         * @param value 具体的每行数据
         * @param context 上下文对象
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(value.getLength()==0){
                return;
            }
            String[] allInfo = value.toString().split("\t");
            String zy = allInfo[7];
            System.out.println("xm:"+allInfo[1]+"--- zy:"+allInfo[7]);
            context.write(new Text(zy),new IntWritable(1));
        }
    }

    /**
     * 统计专业的Reduce类
     */
    static class MajorReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
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
                count += iterator.next().get();
            }
            context.write(key,new IntWritable(count));
        }
    }

}

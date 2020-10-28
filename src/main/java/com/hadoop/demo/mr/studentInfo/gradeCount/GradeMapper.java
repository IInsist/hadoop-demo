package com.hadoop.demo.mr.studentInfo.gradeCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author fujh
 * @since 2020年10月28日09:24:56
 * 学生信息之年级统计
 *
 * 分析student_info.txt文件的Mapper类
 *  每行数据都会对应生成一个map（分片）然后在自定的map方法中进行分析
 *
 *  Mapper<LongWritable, Text,Text, IntWritable>
 *      1、LongWritable输入每行数据的偏移量
 *      2、Text输入每行数据具体内容
 *      3、Text输出数据的KEY
 *      4、IntWritable输出数据的Value
 */
public class GradeMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key 每行数据的偏移量（每行数据的位置）
     * @param value 具体每行的数据内容
     * @param context 上下文的对象，提供输出至Reduce类
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(value.getLength()==0){
            return;
        }
        String[] studentAllInfo = value.toString().split("\t");
        //年级信息
        String nj = studentAllInfo[4];

        System.out.println("xm:"+studentAllInfo[1]+"---"+"nj:"+nj);
        context.write(new Text(nj),new IntWritable(1));
        /**
         * 将拆分的数据依次写入上下文对象中，且记录出现次数为1
         */
        /*for (String temp: studentAllInfo){
            context.write(new Text(temp),new IntWritable(1));
        }*/
    }
}

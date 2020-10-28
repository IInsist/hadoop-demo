package com.hadoop.demo.mr.studentInfo.gradeCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author fujh
 * @since 2020年10月28日09:39:04
 * 学生信息之年级统计
 *
 * 分析stdent_info.txt文件的Reduce类
 */
public class GradeReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     *
     * @param key 由mapper类分析每个数据的KEY
     * @param values 由mapper类分析每个数据的Value;  key和value组合为：(张某某,1),(谢某某,1) ...
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
            IntWritable tmp = iterator.next();
            //将出现的次数进行归总
            count += tmp.get();
        }
        context.write(key,new IntWritable(count));
    }
}

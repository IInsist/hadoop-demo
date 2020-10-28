# hadoop-demo
&nbsp;&nbsp;本demo是记录对对HDFS分布式文件系统的Java API的一些操作。<br>
#####一、相关依赖及版本
```java 
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-hdfs</artifactId>
    <version>2.6.0</version>
</dependency>

<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.6.0</version>
</dependency>

<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>2.6.0</version>
<dependency>
```

#####二、HDFS相关信息<br>
1. HDFS配置类：com.hadoop.demo.config.HdfsConfig
2. HDFS业务实现类：com.hadoop.demo.hdfs.service.impl.HdfsServiceImpl
3. HDFS测试类：com.hadoop.demo.hdfs.HdfsTest

#####三、MapReduce相关信息<br>
1. MapReduce作业所在包：com.hadoop.demo.mr <br>
    注：分析的文件为: data/input/student_info.txt
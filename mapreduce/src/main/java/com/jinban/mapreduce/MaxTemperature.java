package com.jinban.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * 在气象数据集中找出最高气温的应用程序
 * <p>
 * 第三部分: 运行的是MapReduce作业
 * <p>
 * Created by Mr Wong on 2020-11-30 15:39
 */
public class MaxTemperature {

    public static void main(String[] args) throws IOException {
        action1(args);
    }

    private static void action1(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("程序异常终止");
            System.exit(-1);
        }
        JobConf conf = new JobConf(MaxTemperature.class);
        conf.setJobName("Max Temperature");

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(MaxTemperatureMapper.class);
        conf.setReducerClass(MaxTemperatureReducer.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
    }
}

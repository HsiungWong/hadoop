package com.jinban.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * 最高气温示例的mapper接口
 * <p>
 * Map函数接口
 * <p>
 * map函数是由一个Mapper接口来实现的,其中声明了一个map方法
 * <p>
 * Created by Mr Wong on 2020-11-30 14:51
 */
public class MaxTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    /*
    MapReduce组件: 一个Map函数、一个Reduce函数、一些运行作业的代码
     */

    /**
     * map函数的实现, 此方法有4个泛型参数类型
     *
     * @param key      输入键
     * @param value    输入值
     * @param output   输出键,输出值
     * @param reporter unknown
     */
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String year = line.substring(15, 19);
        int airTemperature;
        if (line.charAt(87) == '+') {  //parseInt 前不能有+号
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }
        String quality = line.substring(92, 93);
        if (airTemperature != MISSING && quality.matches("[01429]")) {
            output.collect(new Text(year), new IntWritable(airTemperature));
        }
    }
}

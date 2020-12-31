package com.jinban.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * 最高气温示例的Reducer
 * <p>
 * Reduce函数接口
 * <p>
 * reduce函数在使用reducer时被定义
 * <p>
 * Created by Mr Wong on 2020-11-30 15:27
 */
public class MaxTemperatureReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 4个泛型参数类型用于指定reduce函数的输入和输出类型,reduce函数的输入类型必须和map函数的输出类型相匹配
     */
    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int maxValue = Integer.MAX_VALUE;
        while (values.hasNext()) {
            maxValue = Math.max(maxValue, values.next().get());
        }
        output.collect(key, new IntWritable(maxValue));
    }
}

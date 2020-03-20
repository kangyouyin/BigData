package com.kyy.bigdata.hadoop.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by kangyouyin on 2020/3/10.
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text word = new Text();
    private static final IntWritable one = new IntWritable(1);

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(value.toString());
        while (st.hasMoreTokens()) {
            word.set(st.nextToken());
            context.write(word, one);
        }
    }
}

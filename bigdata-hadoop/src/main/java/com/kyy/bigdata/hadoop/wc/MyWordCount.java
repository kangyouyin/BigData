package com.kyy.bigdata.hadoop.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by kangyouyin on 2020/3/10.
 */
public class MyWordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);

        //让框架知道异构平台运行
//        conf.set("mapreduce.app-submission.cross-platform","true");
//        conf.set("mapreduce.framework.name","local");

        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(conf, args);
        String[] others = genericOptionsParser.getRemainingArgs();

        Job job = Job.getInstance(conf);
        job.setJar("/Users/kangyouyin/IdeaProjects/BigData/bigdata-hadoop/target/bigdata-hadoop-1.0-SNAPSHOT.jar");
        job.setJarByClass(MyWordCount.class);

        job.setJobName("word count");

        Path inFile = new Path("/temp/test.txt");
        TextInputFormat.addInputPath(job, inFile);

        Path outFile = new Path("/temp/result");
        if (outFile.getFileSystem(conf).exists(outFile)) {
            outFile.getFileSystem(conf).delete(outFile, true);
        }
        TextOutputFormat.setOutputPath(job, outFile);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReduce.class);


        job.waitForCompletion(true);
    }
}

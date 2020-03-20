package com.kyy.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * Created by kangyouyin on 2020/1/21.
 */
public class TestHDFS {

    public Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void conn() throws IOException, InterruptedException {
        conf = new Configuration(true);
        fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "root");
    }

    @Test
    public void read() throws IOException {
        Path file = new Path("/temp/test.txt");
        System.out.println( "READING ============================" );
        FSDataInputStream is = fs.open(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        // 示例仅读取一行
        String content = br.readLine();
        System.out.println(content);
        br.close();
    }


    @Test
    public void mkdir() throws IOException {
        Path dir = new Path("kyy01");
        if (fs.exists(dir)) {
            fs.delete(dir, true);
        }
        fs.create(dir);
    }

    /**
     * 上传文件失败，size=0
     * 原因：不在一个网络中，内网IP，也有可能是端口号没放开
     * 查找办法：一定要配日志！！一定要配日志！！一定要配日志！！
     */
    @Test
    public void upload() throws Exception {
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("/Users/kangyouyin/tmp/file/test.txt")));
        Path path = new Path("kyy/out.txt");
        FSDataOutputStream output = fs.create(path);
        System.out.println("1!");
        IOUtils.copyBytes(input, output, conf, true);
        System.out.println("success!");
//        BufferedInputStream input = null;
//        FSDataOutputStream output = null;
//
//        input = new BufferedInputStream(new FileInputStream(new File("/Users/kangyouyin/tmp/file/test.txt")));
//        Path outfile = new Path("/msb/out.txt");
//        output = fs.create(outfile);
//        System.out.println("1!");
//        IOUtils.copyBytes(input, output, conf, true);
//        System.out.println("success!");

    }

    @Test
    public void blocks() throws Exception {

        Path file = new Path("/temp/test.txt");
        FileStatus fss = fs.getFileStatus(file);
        BlockLocation[] blks = fs.getFileBlockLocations(fss, 0, fss.getLen());
        for (BlockLocation b : blks) {
            System.out.println(b);
        }

        FSDataInputStream in = fs.open(file);
        System.out.println("start——————");
        in.seek(5);
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());

    }

    @After
    public void close() throws Exception {
        fs.close();
    }
}

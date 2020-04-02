package com.yaomalang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {

    @Test
    public void createPath() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://localhost:9000");

        FileSystem fs = FileSystem.get(uri, conf, "yaomalang");

        fs.mkdirs(new Path("/input"));
        fs.close();
    }

    @Test
    public void deletePath() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://localhost:9000");

        FileSystem fs = FileSystem.get(uri, conf, "yaomalang");

        fs.delete(new Path("/input"), true);
        fs.close();
    }

    @Test
    public void copyFromLocal() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("dfs.replication", 1);

        URI uri = new URI("hdfs://localhost:9000");

        FileSystem fs = FileSystem.get(uri, conf, "yaomalang");
        fs.copyFromLocalFile(new Path("/Users/yaomalang/Workspace/Enmotech/电子渠道分析报告/sdclog/applog/app.log"), new Path("/input/app.log"));
        fs.close();
    }

}

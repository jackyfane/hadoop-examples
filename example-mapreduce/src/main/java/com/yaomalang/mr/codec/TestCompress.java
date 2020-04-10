package com.yaomalang.mr.codec;

import static org.apache.hadoop.util.ReflectionUtils.newInstance;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

/**
 * TestCompress
 */
public class TestCompress {

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        Class codecClass = Class.forName("org.apache.hadoop.io.compress.BZip2Codec");
        CompressionCodec codec = (CompressionCodec) newInstance(codecClass, new Configuration());

        compress("/Users/yaomalang/Documents/hadoop/input/text2.txt", codec);
        decompress("/Users/yaomalang/Documents/hadoop/input/text2.txt.bz2", codec);
    }

    /**
     * compres file
     * 
     * @param filePath
     * @param codec
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private static void compress(String filePath, CompressionCodec codec) throws IOException {

        FileInputStream fis = new FileInputStream(new File(filePath));

        FileOutputStream fos = new FileOutputStream(filePath + codec.getDefaultExtension());
        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);

        cos.close();
        fos.close();
        fis.close();
    }

    /**
     * decompress
     * 
     * @param filePath
     * @param codec
     * @throws IOException
     */
    private static void decompress(String filePath, CompressionCodec codec) throws IOException {

        FileInputStream fis = new FileInputStream(filePath);
        CompressionInputStream cis = codec.createInputStream(fis);

        String file = filePath.substring(0, filePath.lastIndexOf("."));
        FileOutputStream fos = new FileOutputStream(new File(file));

        IOUtils.copyBytes(cis, fos, 1024 * 1024 * 5, false);

        cis.close();
        fis.close();
        fos.close();
    }

}
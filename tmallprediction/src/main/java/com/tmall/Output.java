package com.tmall;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Output {
    
    public static JavaSparkContext createSparkContext(String appName, String... args) {
        SparkConf conf = new SparkConf().setAppName(appName);
        // 如果最后一个参数是 LOCAL，则设为本地模式
        if (args.length > 0 && "LOCAL".equals(args[args.length - 1])) {
            conf.setMaster("local[*]");
        }
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        return sc;
    }

    public static FileSystem getFileSystem(JavaSparkContext sc) {
        try {
            Configuration conf = sc.hadoopConfiguration();
            return FileSystem.get(
                new java.net.URI(conf.get("fs.defaultFS", "file:///")),
                conf
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize FileSystem", e);
        }
    }

    public static void mergeOutput(JavaSparkContext sc, String dirPath) {
        try {
            FileSystem fs = getFileSystem(sc);
            
            Path outputDir = new Path(dirPath);
            Path partFile = new Path(dirPath + "/part-00000");
            Path tmpFile = new Path(dirPath + ".tmp");
            
            if (fs.exists(partFile)) {
                fs.rename(partFile, tmpFile);
                fs.delete(outputDir, true);
                fs.rename(tmpFile, new Path(dirPath));
                System.out.println(">> [INFO] 文件合并成功: " + dirPath);
            }
        } catch (Exception e) {
            System.out.println(">> [WARN] 文件合并失败: " + e.getMessage());
        }
    }
}
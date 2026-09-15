package com.tmall;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

public class Task1{

    
    public static void main(String[] args) {
        // args[0]=输入路径, args[1]=输出目录, args[2]=LOCAL(可选)
        if (args.length < 2) {
            System.err.println("用法: spark-submit --class com.tmall.Task1 jar <INPUT> <OUTPUT_DIR> [LOCAL]");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputDir = args[1];
        JavaSparkContext sc = Output.createSparkContext("TmallPrediction_Task1", args);

        String outputLogPath = outputDir + "/clean_behavior_log.csv";
        String outputFeaturePath = outputDir + "/user_features.csv";

        System.out.println(">> [INFO] 输入路径: " + inputPath);
        System.out.println(">> [INFO] 输出日志路径: " + outputLogPath);
        System.out.println(">> [INFO] 输出特征路径: " + outputFeaturePath);
        System.out.println("================ TASK1 运行调试日志 ================");
        
        JavaRDD<String> rawlines=sc.textFile(inputPath);

        //Step1:数据清洗，过滤掉不合法的数据
        JavaRDD<String> cleanLines=rawlines.filter(line->{
            if(line==null || line.trim().isEmpty()){
                return false;
            }
            String[] fields=line.split(",");
            if(fields.length!=5){
                return false;
            }
            String beh=fields[3].trim().toLowerCase();
            return beh.equals("pv") || beh.equals("cart") || beh.equals("buy")|| beh.equals("fav");
        });
        cleanLines.cache();
        cleanLines.coalesce(1).saveAsTextFile(outputLogPath);
        Output.mergeOutput(sc,outputLogPath);
        long cleanCount = cleanLines.count();
        System.out.println(">> [DEBUG] 数据清洗完成，合法日志总行数: " + cleanCount);
        //Step2:特征工程，提取用户行为特征
        long maxtime=cleanLines.map(line->{
            String[] fields=line.split(",");
            return Long.parseLong(fields[4].trim());
        }).reduce((a,b)->Math.max(a,b));
        System.out.println(">> [DEBUG] 全局最大时间戳 (MAX TIMESTAMP): " + maxtime);

        JavaPairRDD<String,Tuple3<Long,Long,Long>>userbehaviorPairs=cleanLines.mapToPair(line ->{
            String[] fields=line.split(",");
            String userId=fields[0].trim();
            long timestamp=Long.parseLong(fields[4].trim());
            String behavior=fields[3].trim().toLowerCase();
            long score=1;
            if(behavior.equals("buy")){
                score=4;
            }else if(behavior.equals("cart")){
                score=3;
            }else if(behavior.equals("fav")){
                score=2;
            }
            return new Tuple2<>(userId,new Tuple3<>(timestamp,1L,score));
        });

        JavaPairRDD<String,Tuple3<Long,Long,Long>>userFeatures=userbehaviorPairs.reduceByKey((a,b)->{
            long latestTime=Math.max(a._1(), b._1());
            long totalCount=a._2()+b._2();
            long totalScore=a._3()+b._3();
            return new Tuple3<>(latestTime,totalCount,totalScore);
        });
        long userCount = userFeatures.count();
        System.out.println(">> [DEBUG] 特征聚合完成，去重后的有效电商用户总数: " + userCount);
        JavaPairRDD<String,Tuple3<Double,Double,Double>>userFeatureVectors=userFeatures.mapValues(tuple->{
            double f1=(double)(maxtime-tuple._1())/(24*3600);
            double f2=tuple._2().doubleValue();
            double f3=tuple._3().doubleValue();
            return new Tuple3<>(f1,f2,f3);
        });
        userFeatureVectors.cache();

        // Step3:MinMax归一化
        Tuple3<Double,Double,Double>maxmetrics=userFeatureVectors.values().reduce((a,b)->{
            double maxf1=Math.max(a._1(), b._1());
            double maxf2=Math.max(a._2(), b._2());
            double maxf3=Math.max(a._3(), b._3());
            return new Tuple3<>(maxf1,maxf2,maxf3);
        });
        Tuple3<Double,Double,Double>minmetrics=userFeatureVectors.values().reduce((a,b)->{
            double minf1=Math.min(a._1(), b._1());
            double minf2=Math.min(a._2(), b._2());
            double minf3=Math.min(a._3(), b._3());
            return new Tuple3<>(minf1,minf2,minf3);
        });
        final double minf1=minmetrics._1(),minf2=minmetrics._2(),minf3=minmetrics._3();
        final double maxf1=maxmetrics._1(),maxf2=maxmetrics._2(),maxf3=maxmetrics._3();
        System.out.println(String.format(">> [DEBUG] 归一化极值边界 - F1(时效天数) Max: %.4f, Min: %.4f", maxf1, minf1));
        System.out.println(String.format(">> [DEBUG] 归一化极值边界 - F2(交互次数) Max: %.4f, Min: %.4f", maxf2, minf2));
        System.out.println(String.format(">> [DEBUG] 归一化极值边界 - F3(行为总分) Max: %.4f, Min: %.4f", maxf3, minf3));
        JavaRDD<String> finalnormalize=userFeatureVectors.map(pair -> {
            String userId=pair._1();
            Tuple3<Double,Double,Double> features=pair._2();
            double f1=features._1(),f2=features._2(),f3=features._3();
            double normf1=(maxf1==minf1)?0.0:(f1-minf1)/(maxf1-minf1);
            double normf2=(maxf2==minf2)?0.0:(f2-minf2)/(maxf2-minf2);
            double normf3=(maxf3==minf3)?0.0:(f3-minf3)/(maxf3-minf3);
            return String.format("%s,%.4f,%.4f,%.4f",userId,normf1,normf2,normf3);
        });
        finalnormalize.coalesce(1).saveAsTextFile(outputFeaturePath);
        Output.mergeOutput(sc,outputFeaturePath);
        System.out.println(">> [INFO] 任务一数据挖掘链路与归一化计算成功结束！");
        System.out.println("====================================================");
        sc.close();
    }
    
}

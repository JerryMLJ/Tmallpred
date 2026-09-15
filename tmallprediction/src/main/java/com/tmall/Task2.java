package com.tmall;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
public class Task2 {
    public static void main(String[] args){
        // args[0]=输入路径, args[1]=输出目录, args[2]=LOCAL(可选)
        if (args.length < 2) {
            System.err.println("用法: spark-submit --class com.tmall.Task2 jar <INPUT> <OUTPUT_DIR> [LOCAL]");
            System.exit(1);
        }
        String inputLogPath = args[0];
        String outputDir = args[1];
        JavaSparkContext sc = Output.createSparkContext("TmallPrediction_Task2", args);

        String outputInPath = outputDir + "/item_user_inverted_index.txt";
        String outputCoPath = outputDir + "/item_co_occurrence.txt";
        System.out.println(">> [INFO] 输入路径: " + inputLogPath);
        System.out.println(">> [INFO] 倒排索引输出路径: " + outputInPath);
        System.out.println(">> [INFO] 共现输出路径: " + outputCoPath);
        System.out.println("================ TASK2 运行调试日志 ================");
        JavaRDD<String> cleanLines=sc.textFile(inputLogPath);
        //Step1:构建物品-用户倒排索引
        JavaRDD<String[]>deepInt=cleanLines.map(line->line.split(",")).filter(fields->{
            if(fields.length<5){
                return false;
            }
            String behavior=fields[3];
            return "cart".equals(behavior)||"fav".equals(behavior)||"buy".equals(behavior);
        });
        JavaPairRDD<String,String>itemuserPair=deepInt.mapToPair(fields->new Tuple2<>(fields[1].trim(),fields[0].trim())).distinct();
        JavaPairRDD<String,String>invertIndex=itemuserPair.groupByKey().mapValues(users->{
            StringBuilder sb=new StringBuilder();
            for(String user:users){
                if(sb.length()>0){
                    sb.append(",");
                }
                sb.append(user);
            }
            return sb.toString();
        });
        JavaRDD<String>invertIndexLines=invertIndex.map(tuple->tuple._1+"\t"+tuple._2);
        invertIndexLines.coalesce(1).saveAsTextFile(outputInPath);
        Output.mergeOutput(sc,outputInPath);
        System.out.println(">>[SUCCESS]Item-User Inverted Index saved to:"+outputInPath);

        //Step2:计算商品两两共现频次
        JavaPairRDD<String,String>userItemPair=deepInt.mapToPair(fields->new Tuple2<>(fields[0].trim(),fields[1].trim())).distinct();
        JavaPairRDD<String,Iterable<String>>userItemsGroup=userItemPair.groupByKey();
        JavaPairRDD<String,Integer>itemCoOccurrence=userItemsGroup.flatMapToPair(tuple->{
            List<Tuple2<String,Integer>> coOccurList=new ArrayList<>();
            Set<String> uniqueItems=new HashSet<>();
            for(String item:tuple._2){
                uniqueItems.add(item);
            }
            List<String> items=new ArrayList<>(uniqueItems);
            
            if(items.size()<2){
                return coOccurList.iterator();
            }
            for(int i=0;i<items.size();i++){
                for(int j=i+1;j<items.size();j++){
                    String itemA=items.get(i);
                    String itemB=items.get(j);
                    String pairKey=itemA.compareTo(itemB)<0 ? itemA+","+itemB : itemB+","+itemA;

                    coOccurList.add(new Tuple2<>(pairKey,1));
                }
            }
            return coOccurList.iterator();
        }).reduceByKey((a,b)->a+b);
        JavaPairRDD<Integer,String>sorted=itemCoOccurrence.mapToPair(tuple->new Tuple2<>(tuple._2,tuple._1)).sortByKey(false);
        List<Tuple2<Integer,String>>top1000=sorted.take(1000);
        List<String>coOccurLines=new ArrayList<>();
        for(Tuple2<Integer,String> tuple:top1000){
            coOccurLines.add(tuple._2+"\t"+tuple._1);
        }
        JavaRDD<String>coOccurRDD=sc.parallelize(coOccurLines);
        coOccurRDD.coalesce(1).saveAsTextFile(outputCoPath);
        Output.mergeOutput(sc,outputCoPath);
        System.out.println(">>[SUCCESS]Top 1000 Item Co-Occurrence saved to:"+outputCoPath);
        sc.close();
    }
}
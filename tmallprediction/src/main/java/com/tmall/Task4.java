package com.tmall;

import java.util.Collections;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.lang.StringBuilder;

import org.apache.spark.broadcast.Broadcast;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.*;
public class Task4{
        public static void main(String[] args){
                // args[0]=输入路径, args[1]=输出目录, args[2]=目标用户ID(可选,默认602530), args[3]=LOCAL(可选)
                if (args.length < 2) {
                    System.err.println("用法: spark-submit --class com.tmall.Task4 jar <INPUT> <OUTPUT_DIR> [TARGET_USER_ID] [LOCAL]");
                    System.exit(1);
                }
                String inputPath = args[0];
                String outputDir = args[1];
                String ID = args.length >= 3 ? args[2] : "602530";
                JavaSparkContext sc = Output.createSparkContext("TmallPrediction_Task4", args);

                sc.setLogLevel("ERROR");

                String outputPath = outputDir + "/personal_rank_recommendations.txt";
                System.out.println(">> [INFO] 输入路径: " + inputPath);
                System.out.println(">> [INFO] 输出路径: " + outputPath);
                System.out.println(">> [INFO] 目标用户ID: " + ID);
                final String targetUserId="U_"+ID;
                double alpha=0.85;
                int maxIter=20;
                System.out.println("================ TASK4 运行调试日志 ================");

                //Step1:读取Task1文件并过滤深度交互
                JavaRDD<String> cleanLines=sc.textFile(inputPath);
                JavaPairRDD<String,String> userItemPairs=cleanLines
                        .filter(line->{
                                String[] fields=line.split(",");
                                if(fields.length<4){
                                        return false;
                                }
                                String behaviorType=fields[3].trim();
                                return behaviorType.equals("cart") || behaviorType.equals("fav")|| behaviorType.equals("buy");
                        })
                        .mapToPair(line->{
                                String[] fields=line.split(",");
                                String userId="U_"+fields[0].trim();
                                String itemId="I_"+fields[1].trim();
                                return new Tuple2<>(userId,itemId);
                        }).distinct().cache();
                System.out.println("[DEBUG 1] 过滤去重后的有效交互总数: " + userItemPairs.count());
                List<String> userItemList=userItemPairs
                        .filter(tuple->tuple._1.equals(targetUserId))
                        .map(Tuple2::_2)
                        .collect();
                System.out.println("[DEBUG 2] 目标用户 " + targetUserId + " 已购买/加购的商品集合: " + userItemList);
                final Broadcast<List<String>> bcUserItemList=sc.broadcast(userItemList);
                //Step2:构建User-Item二分图
                
                //计算User的出度并分配概率
                JavaPairRDD<String,Tuple2<String,Double>>userToItemsEdges=userItemPairs
                        .groupByKey()
                        .flatMapToPair(tuple->{
                                List<String> items=new ArrayList<>();
                                for(String item:tuple._2){
                                items.add(item);
                                }
                                double temp=1.0/items.size();
                                List<Tuple2<String,Tuple2<String,Double>>> edges=new ArrayList<>();
                                for(String item:items){
                                edges.add(new Tuple2<>(tuple._1,new Tuple2<>(item,temp)));
                                }
                                return edges.iterator();
                        });
                //计算Item的出度并分配概率
                JavaPairRDD<String,Tuple2<String,Double>>itemToUsersEdges=userItemPairs
                .mapToPair(tuple->new Tuple2<>(tuple._2,tuple._1))//反转为Item-User
                        .groupByKey()
                        .flatMapToPair(tuple->{
                                List<String> users=new ArrayList<>();
                                for(String user:tuple._2){
                                users.add(user);
                                }
                                double temp=1.0/users.size();
                                List<Tuple2<String,Tuple2<String,Double>>> edges=new ArrayList<>();
                                for(String user:users){
                                edges.add(new Tuple2<>(tuple._1,new Tuple2<>(user,temp)));
                                }
                                return edges.iterator();
                        });
                //合并边
                JavaPairRDD<String,Tuple2<String,Double>> graphEdges=userToItemsEdges.union(itemToUsersEdges).cache();
                System.out.println("[DEBUG 3] 双向图中的总边数: " + graphEdges.count());
                //获取所有唯一的节点集
                JavaPairRDD<String,Double> allNodes=graphEdges.flatMap(tuple->{
                        List<String> nodes=new ArrayList<>();
                        nodes.add(tuple._1);
                        nodes.add(tuple._2._1);
                        return nodes.iterator();
                }).distinct().mapToPair(node->new Tuple2<>(node,0.0)).cache();
                System.out.println("[DEBUG 4] 全图唯一个节点总数: " + allNodes.count());
                //Step3:初始化
                JavaPairRDD<String,Double> rankRDD=allNodes.mapToPair(tuple->{
                        String node=tuple._1;
                        double rank=node.equals(targetUserId) ? 1.0 : 0.0;
                        return new Tuple2<>(node,rank);
                }).cache();

                //Step4:迭代计算
                for(int iter=0;iter<maxIter;iter++){
                        //Join计算新Rank
                        JavaPairRDD<String,Double> newRankRDD=rankRDD.join(graphEdges)
                                .mapToPair(tuple->{
                                        
                                        double rank=tuple._2._1;
                                        String toNode=tuple._2._2._1;
                                        double edgeProb=tuple._2._2._2;
                                        return new Tuple2<>(toNode,rank*edgeProb);
                                });
                        //聚合
                        rankRDD=newRankRDD.reduceByKey(Double::sum)
                                .rightOuterJoin(allNodes)
                                .mapToPair(tuple->{
                                        String node=tuple._1;
                                        double rank=tuple._2._1.orElse(0.0)*alpha;
                                        if(node.equals(targetUserId)){
                                                rank+=1-alpha;
                                        }
                                        return new Tuple2<>(node,rank);
                                });

                }       
                //Step5:过滤掉已交互的Item并输出Top-10推荐
                Set<String> interactedItems=new HashSet<>(bcUserItemList.value());
                List<Tuple2<String,Double>> recommendations=new ArrayList<>(
                        rankRDD
                        .filter(tuple->{
                                String node=tuple._1;
                                return node.startsWith("I_") && !interactedItems.contains(node);
                        }).collect()
                );
                System.out.println("[DEBUG 5] 最终进入Step5筛选的图节点总数: " + recommendations.size());

                Collections.sort(recommendations,(a,b)->b._2.compareTo(a._2));
                List<Tuple2<String,Double>>top10=recommendations.size()>10 ? recommendations.subList(0,10) : recommendations;
                StringBuilder sb=new StringBuilder();
                for(int i=0;i<top10.size();i++){
                        Tuple2<String,Double> rec=top10.get(i);
                        String itemId=rec._1.substring(2); //去掉"I_"前缀
                        sb.append(itemId).append(":").append(String.format("%.3f", rec._2));
                        if(i<top10.size()-1){
                                sb.append(",");
                        }
                }
                String outputLine=ID+"\t"+sb.toString();
                List<String> outputLines=Collections.singletonList(outputLine);
                sc.parallelize(outputLines).coalesce(1).saveAsTextFile(outputPath);
                Output.mergeOutput(sc,outputPath);
                System.out.println(">> [SUCCESS] Personal Rank recommendations saved to: "+outputPath);
                bcUserItemList.destroy();
                sc.close();
        }
        

}



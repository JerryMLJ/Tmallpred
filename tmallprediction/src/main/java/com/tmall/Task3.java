package com.tmall;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
public class Task3{
    public static class NewClass implements Serializable{
        private static final long serialVersionUID=1L;
        public double[] sumfeatures;
        public long count;
        public NewClass(){
            this.sumfeatures=new double[]{0.0,0.0,0.0};   
            this.count=0L;
        }
        public NewClass(double[] sumfeatures,long count){
            this.sumfeatures=sumfeatures;
            this.count=count;
        }
        public void add(double[]feature){
            for(int i=0;i<3;i++){
                sumfeatures[i]+=feature[i];

            }
            count++;
        }
        public NewClass merge(NewClass other){
            for(int i=0;i<3;i++){
                this.sumfeatures[i]+=other.sumfeatures[i];
            }
            this.count+=other.count;
            return this;
        }
        public double[] getCenter(){
            if(count==0){
                return new double[]{0.0,0.0,0.0};
            }
            double[] center=new double[3];
            for(int i=0;i<3;i++){
                center[i]=sumfeatures[i]/count;
            }
            return center;

        }
        @Override
        public String toString(){
            return Arrays.toString(getCenter());
        }
    }
    public static void main(String[] args){
        // args[0]=输入特征路径, args[1]=输出目录, args[2]=LOCAL(可选)
        if (args.length < 2) {
            System.err.println("用法: spark-submit --class com.tmall.Task3 jar <INPUT> <OUTPUT_DIR> [LOCAL]");
            System.exit(1);
        }
        String inputFeaturePath = args[0];
        String outputDir = args[1];
        JavaSparkContext sc = Output.createSparkContext("TmallPrediction_Task3", args);

        String outputCenterPath = outputDir + "/cluster_centers.txt";
        String outputLabelPath = outputDir + "/user_cluster_labels.csv";

        sc.setLogLevel("WARN");
        System.out.println("================ TASK3 运行调试日志 ================");
        JavaRDD<String> featureLines=sc.textFile(inputFeaturePath);
        JavaPairRDD<String,double[]>userFeaturePairs=featureLines.mapToPair(line->{
            String[] fields=line.split(",");
            String userId=fields[0].trim();
            double[] features=new double[3];
            for(int i=0;i<3;i++){
                features[i]=Double.parseDouble(fields[i+1].trim());
            }
            return new Tuple2<>(userId, features);
        }).cache();
        //Step1:随机抽样初始化
        int K=3;
        int maxIterations=20;
        double threshold=1e-4;
        List<double[]> centers=new ArrayList<>();
        List<Tuple2<String,double[]>> sample=userFeaturePairs.takeSample(false, K, 42);
        for(Tuple2<String,double[]> tuple : sample){
            centers.add(tuple._2);
        }

        //Step2:迭代更新
        Broadcast<List<double[]>> broadcastCenters=null;
        for(int iter=0;iter<maxIterations;iter++){
            broadcastCenters=sc.broadcast(centers);
            final Broadcast<List<double[]>> bcCenters=broadcastCenters;
            JavaPairRDD<Integer,NewClass> clusterSums=userFeaturePairs.mapToPair(userPair->{
                
                double[] features=userPair._2;
                List<double[]> currentCenters=bcCenters.value();
                int closestCenter=0;
                double minDist=Double.MAX_VALUE;
                for(int i=0;i<currentCenters.size();i++){
                    double[] center=currentCenters.get(i);
                    double dist=Math.sqrt(
                        Math.pow(features[0]-center[0],2)+
                        Math.pow(features[1]-center[1],2)+
                        Math.pow(features[2]-center[2],2)
                    );
                    if(dist<minDist){
                        minDist=dist;
                        closestCenter=i;
                    }
                }
                
                return new Tuple2<>(closestCenter,new NewClass(features.clone(),1L));

            });

            JavaPairRDD<Integer,NewClass> newClusterSums=clusterSums.aggregateByKey(
                    new NewClass(),(acc,feature)->{
                    double[] sumFeatures=acc.sumfeatures.clone();
                    for(int i=0;i<3;i++){
                        sumFeatures[i]+=feature.sumfeatures[i];
                    }
                    return new NewClass(sumFeatures,acc.count+feature.count);
                },(acc1,acc2)->{
                    return acc1.merge(acc2);
                }
            );
            Map<Integer,NewClass> clusterSumMap=newClusterSums.collectAsMap();
            List<double[]> newCenters=new ArrayList<>();
            double maxShift=0.0;
            for(int i=0;i<K;i++){
                if(clusterSumMap.containsKey(i)){
                    double[] newCenter=clusterSumMap.get(i).getCenter();
                    double Shift=Math.sqrt(
                        Math.pow(newCenter[0]-centers.get(i)[0],2)+
                        Math.pow(newCenter[1]-centers.get(i)[1],2)+
                        Math.pow(newCenter[2]-centers.get(i)[2],2)
                    );
                    maxShift=Math.max(maxShift, Shift);
                    newCenters.add(newCenter);
                }else{
                    newCenters.add(centers.get(i));
                }
            }
            if(maxShift<threshold){
                broadcastCenters.destroy();
                centers=newCenters;
                break;
            }
            centers=newCenters;
            broadcastCenters.destroy();
            //Step3:持久化输出结果

            
        }
        List<String> centerLines=new ArrayList<>();
        for(int i=0;i<K;i++){
            double[] center=centers.get(i);
            String line=i+"\t"+String.format("%.4f,%.4f,%.4f",center[0],center[1],center[2]);
            centerLines.add(line);
        }
        
            sc.parallelize(centerLines).coalesce(1).saveAsTextFile(outputCenterPath);
        Output.mergeOutput(sc,outputCenterPath);

        final Broadcast<List<double[]>> finalBcCenters=sc.broadcast(centers);
        JavaRDD<String> labelLines=userFeaturePairs.map(userPair->{
            List<double[]> currentCenters=finalBcCenters.value();
            int closestCenter=0;
            double minDist=Double.MAX_VALUE;
            double[] features=userPair._2;
            for(int i=0;i<currentCenters.size();i++){
                double dist=Math.sqrt(
                    Math.pow(features[0]-currentCenters.get(i)[0],2)+
                    Math.pow(features[1]-currentCenters.get(i)[1],2)+
                    Math.pow(features[2]-currentCenters.get(i)[2],2)
                );
                if(dist<minDist){
                    minDist=dist;
                    closestCenter=i;
                }
            }
            return userPair._1+","+closestCenter;
        });
        labelLines.coalesce(1).saveAsTextFile(outputLabelPath);
        Output.mergeOutput(sc,outputLabelPath);
        finalBcCenters.destroy();
        System.out.println(">>[SUCCESS]Cluster centers saved to:"+outputCenterPath);
        System.out.println(">>[SUCCESS]User cluster labels saved to:"+outputLabelPath);
        sc.close();
            

    }
}


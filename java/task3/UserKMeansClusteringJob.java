package task3;

import common.SingleFileOutputWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UserKMeansClusteringJob {

    private static final int DEFAULT_K = 3;
    private static final int DEFAULT_MAX_ITER = 20;
    private static final double DEFAULT_EPSILON = 1.0e-6;
    private static final long DEFAULT_SEED = 2026L;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(
                    "Usage: UserKMeansClusteringJob <userFeaturesCsv> <outputDir> [k] [maxIter] [epsilon] [seed]");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputDir = args[1];
        int k = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_K;
        int maxIter = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_MAX_ITER;
        double epsilon = args.length > 4 ? Double.parseDouble(args[4]) : DEFAULT_EPSILON;
        long seed = args.length > 5 ? Long.parseLong(args[5]) : DEFAULT_SEED;

        SparkConf conf = new SparkConf().setAppName("Task3_User_KMeans_Clustering");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> raw = sc.textFile(inputPath);

            JavaRDD<UserFeatureVector> featureRdd = raw
                    .map(UserFeatureVector::parse)
                    .filter(v -> v != null)
                    .cache();

            // Use a sampled subset as the initial centers for a stable K-Means start.
            List<double[]> sampledCenters = featureRdd
                    .map(v -> KMeansUtils.copy(v.features))
                    .takeSample(false, k, seed);

            if (sampledCenters.size() < k) {
                throw new IllegalArgumentException("Not enough feature vectors to initialize " + k + " clusters");
            }

            double[][] centers = new double[k][];
            for (int i = 0; i < k; i++) {
                centers[i] = KMeansUtils.copy(sampledCenters.get(i));
            }

            for (int iter = 0; iter < maxIter; iter++) {
                // Broadcast the current centers so every executor can assign points locally.
                Broadcast<double[][]> broadcastCenters = sc.broadcast(KMeansUtils.copyCenters(centers));

                JavaPairRDD<Integer, ClusterAggregate> clusterStats = featureRdd
                        .mapToPair(vector -> {
                            double[][] currentCenters = broadcastCenters.value();
                            int bestCluster = KMeansUtils.findNearestCluster(vector.features, currentCenters);
                            return new Tuple2<>(bestCluster, ClusterAggregate.from(vector.features));
                        })
                        .reduceByKey(ClusterAggregate::merge);

                Map<Integer, ClusterAggregate> aggregated = clusterStats.collectAsMap();
                broadcastCenters.destroy();

                double[][] nextCenters = KMeansUtils.copyCenters(centers);
                double maxShift = 0.0;

                for (int clusterId = 0; clusterId < k; clusterId++) {
                    ClusterAggregate aggregate = aggregated.get(clusterId);
                    // Keep the previous center if this cluster receives no points in the current round.
                    if (aggregate != null && aggregate.count > 0) {
                        nextCenters[clusterId] = KMeansUtils.average(aggregate.sum, aggregate.count);
                    }
                    double shift = KMeansUtils.distance(centers[clusterId], nextCenters[clusterId]);
                    if (shift > maxShift) {
                        maxShift = shift;
                    }
                }

                centers = nextCenters;
                if (maxShift <= epsilon) {
                    break;
                }
            }

            double[][] finalCenters = KMeansUtils.copyCenters(centers);
            Broadcast<double[][]> finalBroadcast = sc.broadcast(finalCenters);

            // Assign each user to the nearest final center and persist the label table.
            JavaPairRDD<String, Integer> labels = featureRdd
                    .mapToPair(vector -> {
                        int clusterId = KMeansUtils.findNearestCluster(vector.features, finalBroadcast.value());
                        return new Tuple2<>(vector.userId, clusterId);
                    })
                    .sortByKey(true);

            JavaRDD<String> labelLines = labels.map(tuple -> tuple._1 + "," + tuple._2);
            SingleFileOutputWriter.write(labelLines, sc, outputDir + "/user_cluster_labels.csv");

            List<String> centerLines = new ArrayList<>();
            for (int clusterId = 0; clusterId < finalCenters.length; clusterId++) {
                centerLines.add(String.format("%d\t%s", clusterId,
                        KMeansUtils.formatCenter(finalCenters[clusterId])));
            }
            SingleFileOutputWriter.write(sc.parallelize(centerLines), sc, outputDir + "/cluster_centers.txt");

            finalBroadcast.destroy();
        }
    }

}
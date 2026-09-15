package task1;

import common.BehaviorParser;
import common.BehaviorRecord;
import common.SingleFileOutputWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class DataCleaningUserFeatureExtractionJob {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: DataCleaningUserFeatureExtractionJob <inputPath> <outputDir>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputDir = args[1];

        SparkConf conf = new SparkConf().setAppName("Task1_DataCleaning_UserFeatureExtraction");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> raw = sc.textFile(inputPath);

            // parse and filter using BehaviorParser
            JavaRDD<BehaviorRecord> records = raw.map(BehaviorParser::parse).filter(r -> r != null).cache();

            // save cleaned CSV as a single file
            SingleFileOutputWriter.write(records.map(BehaviorRecord::toCsvLine), sc,
                    outputDir + "/clean_behavior_log.csv");

            // global max timestamp
            long globalMaxTs = records.map(r -> r.timestamp).reduce(Math::max);

            // aggregate per-user stats
            JavaPairRDD<String, UserStats> userStats = records
                    .mapToPair(r -> new Tuple2<>(r.userId, UserStats.fromRecord(r)))
                    .reduceByKey((a, b) -> a.merge(b));

            // compute raw features
            JavaPairRDD<String, double[]> userRawFeatures = userStats
                    .mapValues(us -> FeatureUtils.computeRaw(us, globalMaxTs));

            // compute global min/max in a single pass
            double[] initMinMax = new double[] {
                    Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
                    Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY
            };

            double[] minMaxVals = userRawFeatures.values().aggregate(initMinMax,
                    (acc, v) -> {
                        acc[0] = Math.min(acc[0], v[0]);
                        acc[1] = Math.min(acc[1], v[1]);
                        acc[2] = Math.min(acc[2], v[2]);
                        acc[3] = Math.max(acc[3], v[0]);
                        acc[4] = Math.max(acc[4], v[1]);
                        acc[5] = Math.max(acc[5], v[2]);
                        return acc;
                    },
                    (a, b) -> {
                        a[0] = Math.min(a[0], b[0]);
                        a[1] = Math.min(a[1], b[1]);
                        a[2] = Math.min(a[2], b[2]);
                        a[3] = Math.max(a[3], b[3]);
                        a[4] = Math.max(a[4], b[4]);
                        a[5] = Math.max(a[5], b[5]);
                        return a;
                    });

            double[] minVals = new double[] { minMaxVals[0], minMaxVals[1], minMaxVals[2] };
            double[] maxVals = new double[] { minMaxVals[3], minMaxVals[4], minMaxVals[5] };

            // normalize and save
            JavaRDD<String> out = userRawFeatures.map(tuple -> {
                String user = tuple._1;
                double[] rawv = tuple._2;
                double[] norm = FeatureUtils.normalize(rawv, minVals, maxVals);
                return String.format("%s,%.4f,%.4f,%.4f", user, norm[0], norm[1], norm[2]);
            });

            SingleFileOutputWriter.write(out, sc, outputDir + "/user_features.csv");
        }
    }
}
